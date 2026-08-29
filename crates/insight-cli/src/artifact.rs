//! Public Artifact metadata and integrity-checked content download client.

use crate::{
    artifact_journal::{self, ArtifactJournalError, ArtifactUploadJournalV1},
    public_client::{PublicClientError, PublicHttpClient, PublicJsonResponse},
};
use chrono::Utc;
use insight_platform_contracts::{
    canonical_digest, ArtifactPurpose, ArtifactRef, ArtifactState, DataClassification,
    OperationViewV1, PublicJobKind, PublicJobState, PublicJobTarget, ResourceId, ResourceKind,
    Sha256Digest, UtcTimestamp,
};
use reqwest::{
    blocking::{Body, Client},
    header::{CONTENT_LENGTH, CONTENT_TYPE},
    redirect::Policy,
    StatusCode, Url,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::{
    fmt,
    fs::{self, File, OpenOptions},
    io::{Read as _, Write as _},
    path::{Path, PathBuf},
    time::Duration,
};
use uuid::Uuid;

#[derive(Debug)]
pub enum ArtifactClientError {
    InvalidRequest(String),
    InvalidResponse(String),
    Public(PublicClientError),
    Journal(ArtifactJournalError),
    File { path: String, detail: String },
}

impl fmt::Display for ArtifactClientError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRequest(detail) => {
                write!(formatter, "Artifact request is invalid: {detail}")
            }
            Self::InvalidResponse(detail) => {
                write!(
                    formatter,
                    "Artifact authority response is invalid: {detail}"
                )
            }
            Self::Public(error) => write!(formatter, "{error}"),
            Self::Journal(error) => write!(formatter, "{error}"),
            Self::File { path, detail } => {
                write!(
                    formatter,
                    "cannot write Artifact content at {path}: {detail}"
                )
            }
        }
    }
}

impl std::error::Error for ArtifactClientError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Public(error) => Some(error),
            Self::Journal(error) => Some(error),
            _ => None,
        }
    }
}

impl From<PublicClientError> for ArtifactClientError {
    fn from(value: PublicClientError) -> Self {
        Self::Public(value)
    }
}

impl From<ArtifactJournalError> for ArtifactClientError {
    fn from(value: ArtifactJournalError) -> Self {
        Self::Journal(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactViewV1 {
    pub schema_version: u32,
    pub artifact_id: ResourceId,
    pub purpose: ArtifactPurpose,
    pub classification: DataClassification,
    pub state: ArtifactState,
    pub version: u64,
    pub expected_size_bytes: u64,
    pub declared_media_type: Option<String>,
    pub verified_media_type: Option<String>,
    pub content: Option<ArtifactRef>,
    pub retain_until: UtcTimestamp,
    pub created_at: UtcTimestamp,
    pub updated_at: UtcTimestamp,
    pub etag: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ArtifactDownloadReportV1 {
    pub schema_version: u16,
    pub kind: &'static str,
    pub artifact_id: String,
    pub output_path: String,
    pub byte_length: u64,
    pub media_type: String,
    pub content_digest: String,
    pub content_etag: String,
    pub trace_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PrepareArtifactUploadRequestV1 {
    pub schema_version: u32,
    pub purpose: ArtifactPurpose,
    pub classification: DataClassification,
    pub expected_size_bytes: u64,
    pub expected_digest: Option<Sha256Digest>,
    pub declared_media_type: Option<String>,
    pub display_name: Option<String>,
}

#[derive(Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(transparent)]
pub(crate) struct OpaqueUploadCompletionProof(pub(crate) String);

impl fmt::Debug for OpaqueUploadCompletionProof {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("OpaqueUploadCompletionProof([REDACTED])")
    }
}

impl OpaqueUploadCompletionProof {
    fn validate(&self) -> bool {
        !self.0.is_empty()
            && self.0.len() <= 4_096
            && self.0.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'~' | b'-')
            })
    }
}

#[derive(Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SecretBearingUploadTargetV1 {
    pub(crate) url: String,
    pub(crate) completion_proof: OpaqueUploadCompletionProof,
}

impl fmt::Debug for SecretBearingUploadTargetV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SecretBearingUploadTargetV1([REDACTED])")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PrepareArtifactUploadResponseV1 {
    pub(crate) schema_version: u32,
    pub(crate) artifact_id: ResourceId,
    pub(crate) operation_id: ResourceId,
    pub(crate) upload_grant_id: ResourceId,
    pub(crate) artifact_etag: String,
    pub(crate) upload_target: SecretBearingUploadTargetV1,
    pub(crate) upload_expires_at: UtcTimestamp,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
struct CompleteArtifactUploadRequestV1<'a> {
    schema_version: u32,
    completion_proof: &'a OpaqueUploadCompletionProof,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ArtifactMutationAcceptedV1 {
    pub(crate) schema_version: u32,
    pub(crate) artifact_id: ResourceId,
    pub(crate) artifact_etag: String,
    pub(crate) operation_id: ResourceId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactUploadOptions {
    pub purpose: ArtifactPurpose,
    pub classification: DataClassification,
    pub declared_media_type: Option<String>,
    pub display_name: Option<String>,
    pub operation_timeout: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ArtifactUploadReportV1 {
    pub schema_version: u16,
    pub kind: String,
    pub artifact_id: String,
    pub operation_id: String,
    pub upload_grant_id: String,
    pub byte_length: u64,
    pub media_type: String,
    pub content_digest: String,
    pub artifact_etag: String,
}

pub trait ArtifactObjectUploader {
    fn put(
        &self,
        target_url: &str,
        source: &Path,
        content_length: u64,
        content_type: Option<&str>,
    ) -> Result<(), ArtifactClientError>;
}

#[derive(Debug)]
pub struct HttpsArtifactObjectUploader {
    client: Client,
}

impl HttpsArtifactObjectUploader {
    pub fn new() -> Result<Self, ArtifactClientError> {
        let client = Client::builder()
            .redirect(Policy::none())
            .no_proxy()
            .timeout(Duration::from_secs(300))
            .user_agent("insight-cli-artifact-upload/0.1")
            .build()
            .map_err(|_| {
                ArtifactClientError::InvalidRequest(
                    "cannot initialize isolated HTTPS upload client".to_owned(),
                )
            })?;
        Ok(Self { client })
    }
}

impl ArtifactObjectUploader for HttpsArtifactObjectUploader {
    fn put(
        &self,
        target_url: &str,
        source: &Path,
        content_length: u64,
        content_type: Option<&str>,
    ) -> Result<(), ArtifactClientError> {
        validate_secret_upload_url(target_url)?;
        let file = File::open(source).map_err(|error| file_error(source, &error.to_string()))?;
        let mut request = self
            .client
            .put(target_url)
            .header(CONTENT_LENGTH, content_length)
            .body(Body::new(file));
        if let Some(content_type) = content_type {
            request = request.header(CONTENT_TYPE, content_type);
        }
        let response = request.send().map_err(|_| {
            ArtifactClientError::InvalidResponse(
                "secret-bearing object upload transport failed".to_owned(),
            )
        })?;
        if response.status() != StatusCode::OK {
            return Err(ArtifactClientError::InvalidResponse(
                "secret-bearing object upload was not accepted".to_owned(),
            ));
        }
        Ok(())
    }
}

pub fn upload_artifact<U: ArtifactObjectUploader>(
    client: &PublicHttpClient,
    uploader: &U,
    expected_tenant_id: &ResourceId,
    source: &Path,
    options: ArtifactUploadOptions,
    journal_directory: &Path,
) -> Result<ArtifactUploadReportV1, ArtifactClientError> {
    if expected_tenant_id.kind() != ResourceKind::Tenant
        || options.operation_timeout.is_zero()
        || options.operation_timeout > Duration::from_secs(3_600)
    {
        return Err(ArtifactClientError::InvalidRequest(
            "tenant or operation timeout is outside its closed bound".to_owned(),
        ));
    }
    validate_media_type(options.declared_media_type.as_deref())?;
    validate_display_name(options.display_name.as_deref())?;
    let (content_length, content_digest) = describe_file(source)?;
    let request = PrepareArtifactUploadRequestV1 {
        schema_version: 1,
        purpose: options.purpose,
        classification: options.classification,
        expected_size_bytes: content_length,
        expected_digest: Some(content_digest.clone()),
        declared_media_type: options.declared_media_type.clone(),
        display_name: options.display_name,
    };
    let request_digest = canonical_digest(
        &serde_json::to_value(&request)
            .map_err(|error| ArtifactClientError::InvalidRequest(error.to_string()))?,
    )
    .map_err(|error| ArtifactClientError::InvalidRequest(error.to_string()))?
    .parse::<Sha256Digest>()
    .map_err(|error| ArtifactClientError::InvalidRequest(error.to_string()))?;
    let request_digest_text = request_digest.to_string();
    let prepare_receipt = format!(
        "insight-artifact-v1-{}-prepare",
        request_digest_text
            .strip_prefix("sha256:")
            .unwrap_or(&request_digest_text)
    );
    let journal_path = artifact_journal::journal_path(journal_directory, &request_digest);
    let mut journal = match artifact_journal::load(&journal_path)? {
        Some(journal) => {
            validate_upload_journal(&journal, &request_digest, &request, &prepare_receipt)?;
            journal
        }
        None => {
            ArtifactUploadJournalV1::new(request_digest.clone(), request.clone(), prepare_receipt)
        }
    };
    artifact_journal::save(&journal_path, &journal)?;

    if journal.prepared.is_none() {
        let prepared: PublicJsonResponse<PrepareArtifactUploadResponseV1> = client.post_json(
            "/v1/artifacts:prepare-upload",
            &journal.request,
            StatusCode::CREATED,
            &journal.prepare_receipt,
            None,
        )?;
        validate_prepare_response(&prepared)?;
        let authority = prepared.body;
        journal.complete_receipt = Some(complete_receipt(&authority)?);
        journal.prepared = Some(authority);
        artifact_journal::save(&journal_path, &journal)?;
    }
    let mut authority = journal
        .prepared
        .as_ref()
        .expect("prepared phase was persisted")
        .clone();
    validate_persisted_prepare(&authority)?;

    if !journal.object_uploaded && upload_target_expires_within(&authority, 30)? {
        let refreshed: PublicJsonResponse<PrepareArtifactUploadResponseV1> = client.post_json(
            "/v1/artifacts:prepare-upload",
            &journal.request,
            StatusCode::CREATED,
            &journal.prepare_receipt,
            None,
        )?;
        validate_prepare_response(&refreshed)?;
        validate_refreshed_prepare(&authority, &refreshed.body)?;
        authority = refreshed.body;
        journal.complete_receipt = Some(complete_receipt(&authority)?);
        journal.prepared = Some(authority.clone());
        artifact_journal::save(&journal_path, &journal)?;
    }

    if !journal.object_uploaded {
        uploader.put(
            &authority.upload_target.url,
            source,
            content_length,
            options.declared_media_type.as_deref(),
        )?;
        journal.object_uploaded = true;
        artifact_journal::save(&journal_path, &journal)?;
    }

    if journal.completed.is_none() {
        let completion = CompleteArtifactUploadRequestV1 {
            schema_version: 1,
            completion_proof: &authority.upload_target.completion_proof,
        };
        let completed: PublicJsonResponse<ArtifactMutationAcceptedV1> = client.post_json(
            &format!("/v1/artifacts/{}:complete-upload", authority.artifact_id),
            &completion,
            StatusCode::ACCEPTED,
            journal
                .complete_receipt
                .as_deref()
                .expect("complete Receipt"),
            Some(&authority.artifact_etag),
        )?;
        validate_complete_response(&completed, &authority)?;
        journal.completed = Some(completed.body);
        artifact_journal::save(&journal_path, &journal)?;
    }

    if journal.result.is_none() {
        let operation = client.wait_operation(
            &authority.operation_id,
            expected_tenant_id,
            options.operation_timeout,
        )?;
        validate_artifact_operation(&operation, &authority.artifact_id)?;
    }
    let artifact = read_artifact(client, &authority.artifact_id)?;
    let content = artifact.content.as_ref().ok_or_else(|| {
        ArtifactClientError::InvalidResponse(
            "verification succeeded without Ready Artifact content".to_owned(),
        )
    })?;
    if artifact.purpose != request.purpose
        || artifact.classification != request.classification
        || artifact.expected_size_bytes != content_length
        || artifact.declared_media_type != request.declared_media_type
        || content.content_digest() != &content_digest
    {
        return Err(ArtifactClientError::InvalidResponse(
            "Ready Artifact differs from the exact upload request".to_owned(),
        ));
    }
    let report = ArtifactUploadReportV1 {
        schema_version: 1,
        kind: "insight.platform.artifact-upload-report/v1".to_owned(),
        artifact_id: authority.artifact_id.to_string(),
        operation_id: authority.operation_id.to_string(),
        upload_grant_id: authority.upload_grant_id.to_string(),
        byte_length: content.byte_length(),
        media_type: content.media_type().to_owned(),
        content_digest: content.content_digest().to_string(),
        artifact_etag: artifact.etag,
    };
    journal.result = Some(report.clone());
    artifact_journal::save(&journal_path, &journal)?;
    Ok(report)
}

fn complete_receipt(
    authority: &PrepareArtifactUploadResponseV1,
) -> Result<String, ArtifactClientError> {
    let proof_digest = canonical_digest(&serde_json::json!({
        "schema_version": 1,
        "artifact_id": authority.artifact_id,
        "completion_proof": authority.upload_target.completion_proof,
    }))
    .map_err(|error| ArtifactClientError::InvalidRequest(error.to_string()))?;
    Ok(format!(
        "insight-artifact-v1-{}-complete",
        proof_digest
            .strip_prefix("sha256:")
            .unwrap_or(&proof_digest)
    ))
}

fn validate_upload_journal(
    journal: &ArtifactUploadJournalV1,
    request_digest: &Sha256Digest,
    request: &PrepareArtifactUploadRequestV1,
    prepare_receipt: &str,
) -> Result<(), ArtifactClientError> {
    if &journal.request_digest != request_digest
        || &journal.request != request
        || journal.prepare_receipt != prepare_receipt
    {
        return Err(ArtifactClientError::InvalidResponse(
            "Artifact upload journal differs from the deterministic command".to_owned(),
        ));
    }
    if let Some(authority) = &journal.prepared {
        let expected = complete_receipt(authority)?;
        if journal.complete_receipt.as_deref() != Some(expected.as_str()) {
            return Err(ArtifactClientError::InvalidResponse(
                "Artifact upload journal differs from the deterministic command".to_owned(),
            ));
        }
    }
    Ok(())
}

fn validate_persisted_prepare(
    prepared: &PrepareArtifactUploadResponseV1,
) -> Result<(), ArtifactClientError> {
    if prepared.schema_version != 1
        || prepared.artifact_id.kind() != ResourceKind::Artifact
        || prepared.operation_id.kind() != ResourceKind::Job
        || prepared.upload_grant_id.kind() != ResourceKind::ArtifactGrant
        || !valid_strong_etag(&prepared.artifact_etag)
        || !prepared.upload_target.completion_proof.validate()
        || validate_secret_upload_url(&prepared.upload_target.url).is_err()
        || chrono::DateTime::parse_from_rfc3339(prepared.upload_expires_at.as_str()).is_err()
    {
        return Err(ArtifactClientError::InvalidResponse(
            "persisted prepare authority is invalid".to_owned(),
        ));
    }
    Ok(())
}

fn upload_target_expires_within(
    prepared: &PrepareArtifactUploadResponseV1,
    seconds: i64,
) -> Result<bool, ArtifactClientError> {
    let expiry = chrono::DateTime::parse_from_rfc3339(prepared.upload_expires_at.as_str())
        .map_err(|_| ArtifactClientError::InvalidResponse("upload expiry is invalid".to_owned()))?
        .with_timezone(&Utc);
    Ok(expiry <= Utc::now() + chrono::Duration::seconds(seconds))
}

fn validate_refreshed_prepare(
    previous: &PrepareArtifactUploadResponseV1,
    refreshed: &PrepareArtifactUploadResponseV1,
) -> Result<(), ArtifactClientError> {
    if refreshed.artifact_id != previous.artifact_id
        || refreshed.operation_id != previous.operation_id
        || refreshed.upload_grant_id != previous.upload_grant_id
        || refreshed.artifact_etag != previous.artifact_etag
    {
        return Err(ArtifactClientError::InvalidResponse(
            "prepare replay changed the Artifact, Operation, Grant, or generation fence".to_owned(),
        ));
    }
    Ok(())
}

pub fn read_artifact(
    client: &PublicHttpClient,
    artifact_id: &ResourceId,
) -> Result<ArtifactViewV1, ArtifactClientError> {
    require_artifact_id(artifact_id)?;
    let response: PublicJsonResponse<ArtifactViewV1> =
        client.get_json(&format!("/v1/artifacts/{artifact_id}"), StatusCode::OK)?;
    validate_artifact_response(&response, artifact_id)?;
    Ok(response.body)
}

fn validate_prepare_response(
    response: &PublicJsonResponse<PrepareArtifactUploadResponseV1>,
) -> Result<(), ArtifactClientError> {
    let prepared = &response.body;
    let expires_at = chrono::DateTime::parse_from_rfc3339(prepared.upload_expires_at.as_str())
        .map_err(|_| ArtifactClientError::InvalidResponse("upload expiry is invalid".to_owned()))?
        .with_timezone(&Utc);
    if prepared.schema_version != 1
        || prepared.artifact_id.kind() != ResourceKind::Artifact
        || prepared.operation_id.kind() != ResourceKind::Job
        || prepared.upload_grant_id.kind() != ResourceKind::ArtifactGrant
        || prepared.artifact_etag != response.etag
        || !valid_strong_etag(&prepared.artifact_etag)
        || response.location.as_deref()
            != Some(format!("/v1/artifacts/{}", prepared.artifact_id).as_str())
        || !prepared.upload_target.completion_proof.validate()
        || validate_secret_upload_url(&prepared.upload_target.url).is_err()
        || expires_at <= Utc::now()
    {
        return Err(ArtifactClientError::InvalidResponse(
            "prepare response identity, target, expiry, or envelope is invalid".to_owned(),
        ));
    }
    Ok(())
}

fn validate_complete_response(
    response: &PublicJsonResponse<ArtifactMutationAcceptedV1>,
    prepared: &PrepareArtifactUploadResponseV1,
) -> Result<(), ArtifactClientError> {
    let completed = &response.body;
    if completed.schema_version != 1
        || completed.artifact_id != prepared.artifact_id
        || completed.operation_id != prepared.operation_id
        || completed.artifact_etag != response.etag
        || !valid_strong_etag(&completed.artifact_etag)
        || response.location.as_deref()
            != Some(format!("/v1/operations/{}", completed.operation_id).as_str())
    {
        return Err(ArtifactClientError::InvalidResponse(
            "complete response differs from the prepared Artifact authority".to_owned(),
        ));
    }
    Ok(())
}

fn validate_artifact_operation(
    operation: &OperationViewV1,
    artifact_id: &ResourceId,
) -> Result<(), ArtifactClientError> {
    if operation.kind != PublicJobKind::ArtifactVerify
        || !matches!(
            &operation.target,
            PublicJobTarget::Artifact { artifact_id: target } if target == artifact_id
        )
    {
        return Err(ArtifactClientError::InvalidResponse(
            "verification Operation does not target the exact Artifact".to_owned(),
        ));
    }
    if operation.state != PublicJobState::Succeeded {
        return Err(ArtifactClientError::InvalidResponse(format!(
            "verification Operation reached terminal state {}",
            public_job_state_name(operation.state)
        )));
    }
    Ok(())
}

const fn public_job_state_name(state: PublicJobState) -> &'static str {
    match state {
        PublicJobState::Queued => "queued",
        PublicJobState::Running => "running",
        PublicJobState::Waiting => "waiting",
        PublicJobState::Succeeded => "succeeded",
        PublicJobState::Failed => "failed",
        PublicJobState::Cancelled => "cancelled",
        PublicJobState::TimedOut => "timed_out",
        PublicJobState::ReconciliationRequired => "reconciliation_required",
    }
}

fn describe_file(source: &Path) -> Result<(u64, Sha256Digest), ArtifactClientError> {
    let metadata = fs::metadata(source).map_err(|error| file_error(source, &error.to_string()))?;
    if !metadata.is_file() || metadata.len() == 0 || metadata.len() > 1_073_741_824 {
        return Err(ArtifactClientError::InvalidRequest(
            "upload source must be a regular file within 1..=1073741824 bytes".to_owned(),
        ));
    }
    let mut file = File::open(source).map_err(|error| file_error(source, &error.to_string()))?;
    let mut hasher = Sha256::new();
    let mut observed = 0_u64;
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|error| file_error(source, &error.to_string()))?;
        if read == 0 {
            break;
        }
        observed = observed.checked_add(read as u64).ok_or_else(|| {
            ArtifactClientError::InvalidRequest("upload source length overflowed".to_owned())
        })?;
        if observed > metadata.len() {
            return Err(ArtifactClientError::InvalidRequest(
                "upload source changed while hashing".to_owned(),
            ));
        }
        hasher.update(&buffer[..read]);
    }
    if observed != metadata.len() {
        return Err(ArtifactClientError::InvalidRequest(
            "upload source changed while hashing".to_owned(),
        ));
    }
    let mut encoded = String::from("sha256:");
    for byte in hasher.finalize() {
        use std::fmt::Write as _;
        write!(&mut encoded, "{byte:02x}").map_err(|_| {
            ArtifactClientError::InvalidRequest("cannot encode upload digest".to_owned())
        })?;
    }
    let digest = encoded
        .parse::<Sha256Digest>()
        .map_err(|error| ArtifactClientError::InvalidRequest(error.to_string()))?;
    Ok((observed, digest))
}

fn validate_secret_upload_url(value: &str) -> Result<(), ArtifactClientError> {
    let parsed = Url::parse(value).map_err(|_| {
        ArtifactClientError::InvalidResponse("secret-bearing upload target is invalid".to_owned())
    })?;
    if value.len() > 8_192
        || parsed.scheme() != "https"
        || parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.fragment().is_some()
    {
        return Err(ArtifactClientError::InvalidResponse(
            "secret-bearing upload target is outside its closed HTTPS shape".to_owned(),
        ));
    }
    Ok(())
}

fn validate_media_type(value: Option<&str>) -> Result<(), ArtifactClientError> {
    if value.is_some_and(|value| {
        value.is_empty()
            || value.len() > 255
            || !value.is_ascii()
            || value.bytes().any(|byte| byte.is_ascii_control())
            || !value.split(';').next().unwrap_or_default().contains('/')
    }) {
        return Err(ArtifactClientError::InvalidRequest(
            "declared media type is invalid".to_owned(),
        ));
    }
    Ok(())
}

fn validate_display_name(value: Option<&str>) -> Result<(), ArtifactClientError> {
    if value.is_some_and(|value| {
        value.is_empty() || value.len() > 512 || value.chars().any(char::is_control)
    }) {
        return Err(ArtifactClientError::InvalidRequest(
            "display name is invalid".to_owned(),
        ));
    }
    Ok(())
}

fn valid_strong_etag(value: &str) -> bool {
    value.len() >= 3
        && value.len() <= 128
        && value.starts_with('"')
        && value.ends_with('"')
        && !value.starts_with("W/")
}

pub fn download_artifact(
    client: &PublicHttpClient,
    artifact_id: &ResourceId,
    output_path: &Path,
) -> Result<ArtifactDownloadReportV1, ArtifactClientError> {
    if output_path.as_os_str().is_empty() || output_path.exists() {
        return Err(ArtifactClientError::InvalidRequest(
            "output path is empty or already exists".to_owned(),
        ));
    }
    let artifact = read_artifact(client, artifact_id)?;
    let content = artifact.content.as_ref().ok_or_else(|| {
        ArtifactClientError::InvalidResponse("Artifact is not Ready with exact content".to_owned())
    })?;
    let parent = output_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    if !parent.is_dir() {
        return Err(file_error(output_path, "output directory does not exist"));
    }
    let temporary = temporary_path(output_path);
    let result = (|| {
        #[cfg(unix)]
        use std::os::unix::fs::OpenOptionsExt as _;

        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        options.mode(0o600);
        let mut file = options
            .open(&temporary)
            .map_err(|error| file_error(&temporary, &error.to_string()))?;
        let downloaded = client.get_binary_to_writer(
            &format!("/v1/artifacts/{artifact_id}/content"),
            content.byte_length(),
            &mut file,
        )?;
        file.flush()
            .and_then(|_| file.sync_all())
            .map_err(|error| file_error(&temporary, &error.to_string()))?;
        if downloaded.content_length != content.byte_length()
            || downloaded.content_type != content.media_type()
            || downloaded.content_digest != *content.content_digest()
            || downloaded.etag != format!("\"{}\"", content.content_digest())
        {
            return Err(ArtifactClientError::InvalidResponse(
                "download length, media type, digest, or ETag differs from ArtifactRef".to_owned(),
            ));
        }
        fs::hard_link(&temporary, output_path)
            .map_err(|error| file_error(output_path, &error.to_string()))?;
        let _ = fs::remove_file(&temporary);
        Ok(ArtifactDownloadReportV1 {
            schema_version: 1,
            kind: "insight.platform.artifact-download-report/v1",
            artifact_id: artifact_id.to_string(),
            output_path: output_path.display().to_string(),
            byte_length: downloaded.content_length,
            media_type: downloaded.content_type,
            content_digest: downloaded.content_digest.to_string(),
            content_etag: downloaded.etag,
            trace_id: downloaded.trace_id.to_string(),
        })
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn validate_artifact_response(
    response: &PublicJsonResponse<ArtifactViewV1>,
    expected_id: &ResourceId,
) -> Result<(), ArtifactClientError> {
    let artifact = &response.body;
    let content_valid = artifact.content.as_ref().is_none_or(|content| {
        content.artifact_id() == expected_id
            && content.classification() == artifact.classification
            && content.byte_length() == artifact.expected_size_bytes
            && artifact.verified_media_type.as_deref() == Some(content.media_type())
    });
    if artifact.schema_version != 1
        || &artifact.artifact_id != expected_id
        || artifact.version == 0
        || artifact.expected_size_bytes == 0
        || artifact.etag != response.etag
        || artifact.etag != format!("\"{}-{}\"", artifact.artifact_id, artifact.version)
        || artifact.content.is_some() != (artifact.state == ArtifactState::Ready)
        || !content_valid
        || artifact.updated_at.as_str() < artifact.created_at.as_str()
    {
        return Err(ArtifactClientError::InvalidResponse(
            "Artifact identity, state, content, version, ETag, or timestamps are inconsistent"
                .to_owned(),
        ));
    }
    Ok(())
}

fn require_artifact_id(artifact_id: &ResourceId) -> Result<(), ArtifactClientError> {
    if artifact_id.kind() != ResourceKind::Artifact {
        return Err(ArtifactClientError::InvalidRequest(
            "artifact_id is not an Artifact ID".to_owned(),
        ));
    }
    Ok(())
}

fn temporary_path(output_path: &Path) -> PathBuf {
    output_path.with_extension(format!("{}.part", Uuid::now_v7()))
}

fn file_error(path: &Path, detail: &str) -> ArtifactClientError {
    ArtifactClientError::File {
        path: path.display().to_string(),
        detail: detail.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use insight_platform_contracts::{operation_etag, SafeJobResult};
    use sha2::Sha256;
    use std::{
        net::{TcpListener, TcpStream},
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        thread,
        time::Duration,
    };
    use tempfile::TempDir;

    fn id(kind: ResourceKind) -> ResourceId {
        ResourceId::from_uuid_v7(kind, Uuid::now_v7()).unwrap()
    }

    fn digest(bytes: &[u8]) -> Sha256Digest {
        let mut encoded = String::from("sha256:");
        for byte in Sha256::digest(bytes) {
            use std::fmt::Write as _;
            write!(&mut encoded, "{byte:02x}").unwrap();
        }
        encoded.parse().unwrap()
    }

    #[test]
    fn metadata_and_content_download_are_authority_and_digest_bound() {
        let bytes = b"verified artifact body".to_vec();
        let content_digest = digest(&bytes);
        let artifact_id = id(ResourceKind::Artifact);
        let content = ArtifactRef::new(
            artifact_id.clone(),
            content_digest.clone(),
            bytes.len() as u64,
            "text/plain",
            DataClassification::Internal,
            Some("result.txt".to_owned()),
        )
        .unwrap();
        let view = ArtifactViewV1 {
            schema_version: 1,
            artifact_id: artifact_id.clone(),
            purpose: ArtifactPurpose::RunOutput,
            classification: DataClassification::Internal,
            state: ArtifactState::Ready,
            version: 4,
            expected_size_bytes: bytes.len() as u64,
            declared_media_type: Some("text/plain".to_owned()),
            verified_media_type: Some("text/plain".to_owned()),
            content: Some(content),
            retain_until: "2026-09-29T00:00:00.000000Z".parse().unwrap(),
            created_at: "2026-08-29T00:00:00.000000Z".parse().unwrap(),
            updated_at: "2026-08-29T00:00:01.000000Z".parse().unwrap(),
            etag: format!("\"{artifact_id}-4\""),
        };
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let port = listener.local_addr().unwrap().port();
        let server_view = view.clone();
        let server_digest = content_digest.clone();
        let server_id = artifact_id.clone();
        let server = thread::spawn(move || {
            for step in 0..2 {
                let (mut stream, _) = listener.accept().unwrap();
                let head = read_request_head(&mut stream);
                assert_eq!(header_value(&head, "authorization"), Some("Bearer token"));
                if step == 0 {
                    assert!(head.starts_with(&format!("GET /v1/artifacts/{server_id} HTTP/1.1")));
                    write_json_response(&mut stream, &server_view);
                } else {
                    assert!(head
                        .starts_with(&format!("GET /v1/artifacts/{server_id}/content HTTP/1.1")));
                    write!(
                        stream,
                        "HTTP/1.1 200 OK\r\ncontent-type: text/plain\r\ncontent-length: {}\r\ncontent-disposition: attachment\r\ncache-control: no-store, private, max-age=0\r\netag: \"{}\"\r\ntrace-id: 22222222222222222222222222222222\r\nconnection: close\r\n\r\n",
                        bytes.len(), server_digest
                    )
                    .unwrap();
                    stream.write_all(&bytes).unwrap();
                }
            }
        });
        let client = PublicHttpClient::new(
            format!("http://127.0.0.1:{port}"),
            "token".to_owned(),
            Duration::from_secs(2),
        )
        .unwrap();
        let directory = TempDir::new().unwrap();
        let output = directory.path().join("result.txt");
        let report = download_artifact(&client, &artifact_id, &output).unwrap();
        assert_eq!(fs::read(&output).unwrap(), b"verified artifact body");
        assert_eq!(report.content_digest, content_digest.to_string());
        assert!(download_artifact(&client, &artifact_id, &output).is_err());
        server.join().unwrap();
    }

    struct FixtureUploader;

    impl ArtifactObjectUploader for FixtureUploader {
        fn put(
            &self,
            target_url: &str,
            source: &Path,
            content_length: u64,
            content_type: Option<&str>,
        ) -> Result<(), ArtifactClientError> {
            assert_eq!(
                target_url,
                "https://uploads.example/object?signature=secret"
            );
            assert_eq!(fs::read(source).unwrap(), b"upload body");
            assert_eq!(content_length, 11);
            assert_eq!(content_type, Some("text/plain"));
            Ok(())
        }
    }

    struct CountingUploader(Arc<AtomicUsize>);

    impl ArtifactObjectUploader for CountingUploader {
        fn put(
            &self,
            target_url: &str,
            source: &Path,
            content_length: u64,
            content_type: Option<&str>,
        ) -> Result<(), ArtifactClientError> {
            assert_eq!(
                target_url,
                "https://uploads.example/object?signature=secret"
            );
            assert_eq!(fs::read(source).unwrap(), b"upload body");
            assert_eq!(content_length, 11);
            assert_eq!(content_type, Some("text/plain"));
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[test]
    fn upload_uses_isolated_target_then_waits_for_exact_ready_artifact() {
        let directory = TempDir::new().unwrap();
        let source = directory.path().join("input.txt");
        fs::write(&source, b"upload body").unwrap();
        let content_digest = digest(b"upload body");
        let tenant_id = id(ResourceKind::Tenant);
        let artifact_id = id(ResourceKind::Artifact);
        let operation_id = id(ResourceKind::Job);
        let grant_id = id(ResourceKind::ArtifactGrant);
        let prepared_etag = format!("\"{artifact_id}-1\"");
        let completed_etag = format!("\"{artifact_id}-2\"");
        let ready_etag = format!("\"{artifact_id}-4\"");
        let operation_etag = operation_etag(&operation_id.to_string(), 3);
        let prepared = PrepareArtifactUploadResponseV1 {
            schema_version: 1,
            artifact_id: artifact_id.clone(),
            operation_id: operation_id.clone(),
            upload_grant_id: grant_id.clone(),
            artifact_etag: prepared_etag.clone(),
            upload_target: SecretBearingUploadTargetV1 {
                url: "https://uploads.example/object?signature=secret".to_owned(),
                completion_proof: OpaqueUploadCompletionProof("proof_123.safe".to_owned()),
            },
            upload_expires_at: UtcTimestamp::from_datetime(Utc::now() + chrono::Duration::hours(1)),
        };
        let completed = ArtifactMutationAcceptedV1 {
            schema_version: 1,
            artifact_id: artifact_id.clone(),
            artifact_etag: completed_etag,
            operation_id: operation_id.clone(),
        };
        let operation = OperationViewV1 {
            operation_id: operation_id.clone(),
            tenant_id: tenant_id.clone(),
            kind: PublicJobKind::ArtifactVerify,
            target: PublicJobTarget::Artifact {
                artifact_id: artifact_id.clone(),
            },
            state: PublicJobState::Succeeded,
            progress: None,
            result: Some(SafeJobResult {
                result_digest: digest(b"verification"),
            }),
            error: None,
            created_at: "2026-08-29T00:00:00.000000Z".parse().unwrap(),
            updated_at: "2026-08-29T00:00:01.000000Z".parse().unwrap(),
            etag: operation_etag.clone(),
        };
        let content = ArtifactRef::new(
            artifact_id.clone(),
            content_digest.clone(),
            11,
            "text/plain",
            DataClassification::Internal,
            Some("input.txt".to_owned()),
        )
        .unwrap();
        let ready = ArtifactViewV1 {
            schema_version: 1,
            artifact_id: artifact_id.clone(),
            purpose: ArtifactPurpose::RunInput,
            classification: DataClassification::Internal,
            state: ArtifactState::Ready,
            version: 4,
            expected_size_bytes: 11,
            declared_media_type: Some("text/plain".to_owned()),
            verified_media_type: Some("text/plain".to_owned()),
            content: Some(content),
            retain_until: "2026-09-29T00:00:00.000000Z".parse().unwrap(),
            created_at: "2026-08-29T00:00:00.000000Z".parse().unwrap(),
            updated_at: "2026-08-29T00:00:02.000000Z".parse().unwrap(),
            etag: ready_etag.clone(),
        };

        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let port = listener.local_addr().unwrap().port();
        let server_artifact_id = artifact_id.clone();
        let server_operation_id = operation_id.clone();
        let server_digest = content_digest.clone();
        let server = thread::spawn(move || {
            for step in 0..4 {
                let (mut stream, _) = listener.accept().unwrap();
                let (head, body) = read_request(&mut stream);
                assert_eq!(header_value(&head, "authorization"), Some("Bearer token"));
                match step {
                    0 => {
                        assert!(head.starts_with("POST /v1/artifacts:prepare-upload HTTP/1.1"));
                        assert!(header_value(&head, "idempotency-key")
                            .is_some_and(|value| value.ends_with("-prepare")));
                        let request: serde_json::Value = serde_json::from_slice(&body).unwrap();
                        assert_eq!(request["expected_digest"], server_digest.to_string());
                        let trace = request_trace_id(&head);
                        write_json_envelope(
                            &mut stream,
                            "201 Created",
                            &trace,
                            Some(&prepared_etag),
                            Some(&format!("/v1/artifacts/{server_artifact_id}")),
                            &prepared,
                        );
                    }
                    1 => {
                        assert!(head.starts_with(&format!(
                            "POST /v1/artifacts/{server_artifact_id}:complete-upload HTTP/1.1"
                        )));
                        assert_eq!(
                            header_value(&head, "if-match"),
                            Some(prepared_etag.as_str())
                        );
                        assert!(header_value(&head, "idempotency-key")
                            .is_some_and(|value| value.ends_with("-complete")));
                        assert_eq!(
                            serde_json::from_slice::<serde_json::Value>(&body).unwrap()
                                ["completion_proof"],
                            "proof_123.safe"
                        );
                        let trace = request_trace_id(&head);
                        write_json_envelope(
                            &mut stream,
                            "202 Accepted",
                            &trace,
                            Some(&completed.artifact_etag),
                            Some(&format!("/v1/operations/{server_operation_id}")),
                            &completed,
                        );
                    }
                    2 => {
                        assert!(head.starts_with(&format!(
                            "GET /v1/operations/{server_operation_id} HTTP/1.1"
                        )));
                        write_json_envelope(
                            &mut stream,
                            "200 OK",
                            "33333333333333333333333333333333",
                            Some(&operation_etag),
                            None,
                            &operation,
                        );
                    }
                    3 => {
                        assert!(head.starts_with(&format!(
                            "GET /v1/artifacts/{server_artifact_id} HTTP/1.1"
                        )));
                        write_json_envelope(
                            &mut stream,
                            "200 OK",
                            "44444444444444444444444444444444",
                            Some(&ready_etag),
                            None,
                            &ready,
                        );
                    }
                    _ => unreachable!(),
                }
            }
        });
        let client = PublicHttpClient::new(
            format!("http://127.0.0.1:{port}"),
            "token".to_owned(),
            Duration::from_secs(2),
        )
        .unwrap();
        let report = upload_artifact(
            &client,
            &FixtureUploader,
            &tenant_id,
            &source,
            ArtifactUploadOptions {
                purpose: ArtifactPurpose::RunInput,
                classification: DataClassification::Internal,
                declared_media_type: Some("text/plain".to_owned()),
                display_name: Some("input.txt".to_owned()),
                operation_timeout: Duration::from_secs(2),
            },
            &directory.path().join("journals"),
        )
        .unwrap();
        assert_eq!(report.artifact_id, artifact_id.to_string());
        assert_eq!(report.operation_id, operation_id.to_string());
        assert_eq!(report.upload_grant_id, grant_id.to_string());
        assert_eq!(report.content_digest, content_digest.to_string());
        let serialized = serde_json::to_string(&report).unwrap();
        assert!(!serialized.contains("signature=secret"));
        assert!(!serialized.contains("proof_123"));
        assert!(!serialized.contains("Bearer"));
        server.join().unwrap();
    }

    #[test]
    fn upload_replays_complete_after_response_loss_without_second_object_put() {
        let directory = TempDir::new().unwrap();
        let source = directory.path().join("input.txt");
        fs::write(&source, b"upload body").unwrap();
        let content_digest = digest(b"upload body");
        let tenant_id = id(ResourceKind::Tenant);
        let artifact_id = id(ResourceKind::Artifact);
        let operation_id = id(ResourceKind::Job);
        let grant_id = id(ResourceKind::ArtifactGrant);
        let prepared_etag = format!("\"{artifact_id}-1\"");
        let completed_etag = format!("\"{artifact_id}-2\"");
        let ready_etag = format!("\"{artifact_id}-4\"");
        let operation_etag = operation_etag(&operation_id.to_string(), 3);
        let prepared = PrepareArtifactUploadResponseV1 {
            schema_version: 1,
            artifact_id: artifact_id.clone(),
            operation_id: operation_id.clone(),
            upload_grant_id: grant_id.clone(),
            artifact_etag: prepared_etag.clone(),
            upload_target: SecretBearingUploadTargetV1 {
                url: "https://uploads.example/object?signature=secret".to_owned(),
                completion_proof: OpaqueUploadCompletionProof("proof_123.safe".to_owned()),
            },
            upload_expires_at: UtcTimestamp::from_datetime(Utc::now() + chrono::Duration::hours(1)),
        };
        let completed = ArtifactMutationAcceptedV1 {
            schema_version: 1,
            artifact_id: artifact_id.clone(),
            artifact_etag: completed_etag,
            operation_id: operation_id.clone(),
        };
        let operation = OperationViewV1 {
            operation_id: operation_id.clone(),
            tenant_id: tenant_id.clone(),
            kind: PublicJobKind::ArtifactVerify,
            target: PublicJobTarget::Artifact {
                artifact_id: artifact_id.clone(),
            },
            state: PublicJobState::Succeeded,
            progress: None,
            result: Some(SafeJobResult {
                result_digest: digest(b"verification"),
            }),
            error: None,
            created_at: "2026-08-29T00:00:00.000000Z".parse().unwrap(),
            updated_at: "2026-08-29T00:00:01.000000Z".parse().unwrap(),
            etag: operation_etag.clone(),
        };
        let content = ArtifactRef::new(
            artifact_id.clone(),
            content_digest.clone(),
            11,
            "text/plain",
            DataClassification::Internal,
            Some("input.txt".to_owned()),
        )
        .unwrap();
        let ready = ArtifactViewV1 {
            schema_version: 1,
            artifact_id: artifact_id.clone(),
            purpose: ArtifactPurpose::RunInput,
            classification: DataClassification::Internal,
            state: ArtifactState::Ready,
            version: 4,
            expected_size_bytes: 11,
            declared_media_type: Some("text/plain".to_owned()),
            verified_media_type: Some("text/plain".to_owned()),
            content: Some(content),
            retain_until: "2026-09-29T00:00:00.000000Z".parse().unwrap(),
            created_at: "2026-08-29T00:00:00.000000Z".parse().unwrap(),
            updated_at: "2026-08-29T00:00:02.000000Z".parse().unwrap(),
            etag: ready_etag.clone(),
        };

        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let port = listener.local_addr().unwrap().port();
        let server_artifact_id = artifact_id.clone();
        let server_operation_id = operation_id.clone();
        let server = thread::spawn(move || {
            let mut first_complete_receipt = None;
            for step in 0..6 {
                let (mut stream, _) = listener.accept().unwrap();
                let (head, body) = read_request(&mut stream);
                assert_eq!(header_value(&head, "authorization"), Some("Bearer token"));
                match step {
                    0 => {
                        assert!(head.starts_with("POST /v1/artifacts:prepare-upload HTTP/1.1"));
                        let trace = request_trace_id(&head);
                        write_json_envelope(
                            &mut stream,
                            "201 Created",
                            &trace,
                            Some(&prepared_etag),
                            Some(&format!("/v1/artifacts/{server_artifact_id}")),
                            &prepared,
                        );
                    }
                    1 | 2 => {
                        assert!(head.starts_with(&format!(
                            "POST /v1/artifacts/{server_artifact_id}:complete-upload HTTP/1.1"
                        )));
                        assert_eq!(
                            header_value(&head, "if-match"),
                            Some(prepared_etag.as_str())
                        );
                        let receipt = header_value(&head, "idempotency-key")
                            .expect("complete Receipt")
                            .to_owned();
                        if step == 1 {
                            first_complete_receipt = Some(receipt);
                            drop(stream);
                        } else {
                            assert_eq!(Some(receipt), first_complete_receipt);
                            assert_eq!(
                                serde_json::from_slice::<serde_json::Value>(&body).unwrap()
                                    ["completion_proof"],
                                "proof_123.safe"
                            );
                            let trace = request_trace_id(&head);
                            write_json_envelope(
                                &mut stream,
                                "202 Accepted",
                                &trace,
                                Some(&completed.artifact_etag),
                                Some(&format!("/v1/operations/{server_operation_id}")),
                                &completed,
                            );
                        }
                    }
                    3 => write_json_envelope(
                        &mut stream,
                        "200 OK",
                        "33333333333333333333333333333333",
                        Some(&operation_etag),
                        None,
                        &operation,
                    ),
                    4 | 5 => write_json_envelope(
                        &mut stream,
                        "200 OK",
                        "44444444444444444444444444444444",
                        Some(&ready_etag),
                        None,
                        &ready,
                    ),
                    _ => unreachable!(),
                }
            }
        });
        let client = PublicHttpClient::new(
            format!("http://127.0.0.1:{port}"),
            "token".to_owned(),
            Duration::from_secs(2),
        )
        .unwrap();
        let puts = Arc::new(AtomicUsize::new(0));
        let uploader = CountingUploader(Arc::clone(&puts));
        let journals = directory.path().join("journals");
        let options = ArtifactUploadOptions {
            purpose: ArtifactPurpose::RunInput,
            classification: DataClassification::Internal,
            declared_media_type: Some("text/plain".to_owned()),
            display_name: Some("input.txt".to_owned()),
            operation_timeout: Duration::from_secs(2),
        };
        let first = upload_artifact(
            &client,
            &uploader,
            &tenant_id,
            &source,
            options.clone(),
            &journals,
        );
        assert!(matches!(first, Err(ArtifactClientError::Public(_))));
        assert_eq!(puts.load(Ordering::SeqCst), 1);

        let report =
            upload_artifact(&client, &uploader, &tenant_id, &source, options, &journals).unwrap();
        assert_eq!(report.artifact_id, artifact_id.to_string());
        assert_eq!(report.operation_id, operation_id.to_string());
        assert_eq!(puts.load(Ordering::SeqCst), 1);
        let replayed = upload_artifact(
            &client,
            &uploader,
            &tenant_id,
            &source,
            ArtifactUploadOptions {
                purpose: ArtifactPurpose::RunInput,
                classification: DataClassification::Internal,
                declared_media_type: Some("text/plain".to_owned()),
                display_name: Some("input.txt".to_owned()),
                operation_timeout: Duration::from_secs(2),
            },
            &journals,
        )
        .unwrap();
        assert_eq!(replayed, report);
        assert_eq!(puts.load(Ordering::SeqCst), 1);
        let journal = fs::read_to_string(
            fs::read_dir(&journals)
                .unwrap()
                .next()
                .unwrap()
                .unwrap()
                .path(),
        )
        .unwrap();
        assert!(journal.contains("\"object_uploaded\": true"));
        assert!(journal.contains("insight.platform.artifact-upload-report/v1"));
        server.join().unwrap();
    }

    fn read_request_head(stream: &mut TcpStream) -> String {
        let mut bytes = Vec::new();
        let mut buffer = [0_u8; 2048];
        loop {
            let read = stream.read(&mut buffer).unwrap();
            assert!(read > 0);
            bytes.extend_from_slice(&buffer[..read]);
            if let Some(index) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
                return String::from_utf8(bytes[..index + 4].to_vec()).unwrap();
            }
        }
    }

    fn read_request(stream: &mut TcpStream) -> (String, Vec<u8>) {
        let mut bytes = Vec::new();
        let mut buffer = [0_u8; 4096];
        let header_end = loop {
            let read = stream.read(&mut buffer).unwrap();
            assert!(read > 0);
            bytes.extend_from_slice(&buffer[..read]);
            if let Some(index) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
                break index + 4;
            }
        };
        let head = String::from_utf8(bytes[..header_end].to_vec()).unwrap();
        let content_length = header_value(&head, "content-length")
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(0);
        while bytes.len() < header_end + content_length {
            let read = stream.read(&mut buffer).unwrap();
            assert!(read > 0);
            bytes.extend_from_slice(&buffer[..read]);
        }
        (
            head,
            bytes[header_end..header_end + content_length].to_vec(),
        )
    }

    fn header_value<'a>(head: &'a str, name: &str) -> Option<&'a str> {
        head.lines().find_map(|line| {
            let (candidate, value) = line.split_once(':')?;
            candidate.eq_ignore_ascii_case(name).then_some(value.trim())
        })
    }

    fn write_json_response(stream: &mut TcpStream, view: &ArtifactViewV1) {
        let body = serde_json::to_vec(view).unwrap();
        write!(
            stream,
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\ncache-control: no-store, private, max-age=0\r\netag: {}\r\ntrace-id: 11111111111111111111111111111111\r\nconnection: close\r\n\r\n",
            body.len(), view.etag
        )
        .unwrap();
        stream.write_all(&body).unwrap();
    }

    fn request_trace_id(head: &str) -> String {
        header_value(head, "traceparent")
            .unwrap()
            .split('-')
            .nth(1)
            .unwrap()
            .to_owned()
    }

    fn write_json_envelope<T: Serialize>(
        stream: &mut TcpStream,
        status: &str,
        trace_id: &str,
        etag: Option<&str>,
        location: Option<&str>,
        value: &T,
    ) {
        let body = serde_json::to_vec(value).unwrap();
        write!(
            stream,
            "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\ncache-control: no-store, private, max-age=0\r\ntrace-id: {trace_id}\r\n",
            body.len()
        )
        .unwrap();
        if let Some(etag) = etag {
            write!(stream, "etag: {etag}\r\n").unwrap();
        }
        if let Some(location) = location {
            write!(stream, "location: {location}\r\n").unwrap();
        }
        write!(stream, "connection: close\r\n\r\n").unwrap();
        stream.write_all(&body).unwrap();
    }
}
