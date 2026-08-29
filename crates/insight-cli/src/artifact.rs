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
    Certificate, StatusCode, Url,
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
        Self::with_additional_root(None)
    }

    fn with_additional_root(
        additional_root: Option<Certificate>,
    ) -> Result<Self, ArtifactClientError> {
        let mut builder = Client::builder()
            .redirect(Policy::none())
            .no_proxy()
            .timeout(Duration::from_secs(300))
            .user_agent("insight-cli-artifact-upload/0.1");
        if let Some(root) = additional_root {
            builder = builder.add_root_certificate(root);
        }
        let client = builder.build().map_err(|_| {
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
include!("artifact_tests.rs");
