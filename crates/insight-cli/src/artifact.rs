//! Public Artifact metadata and integrity-checked content download client.

use crate::public_client::{PublicClientError, PublicHttpClient, PublicJsonResponse};
use insight_platform_contracts::{
    ArtifactPurpose, ArtifactRef, ArtifactState, DataClassification, ResourceId, ResourceKind,
    UtcTimestamp,
};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use std::{
    fmt,
    fs::{self, OpenOptions},
    io::Write as _,
    path::{Path, PathBuf},
};
use uuid::Uuid;

#[derive(Debug)]
pub enum ArtifactClientError {
    InvalidRequest(String),
    InvalidResponse(String),
    Public(PublicClientError),
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
            _ => None,
        }
    }
}

impl From<PublicClientError> for ArtifactClientError {
    fn from(value: PublicClientError) -> Self {
        Self::Public(value)
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
    use insight_platform_contracts::Sha256Digest;
    use sha2::{Digest as _, Sha256};
    use std::{
        io::Read as _,
        net::{TcpListener, TcpStream},
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
}
