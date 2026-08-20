use crate::{read_guest_frame, write_guest_frame, MicroVmGuestProtocolError};
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use chrono::{DateTime, Utc};
use insight_platform_contracts::{
    canonical_digest, ArtifactRef, ResourceId, ResourceKind, SecretPurpose, Sha256Digest,
};
use insight_platform_jobs::JobFence;
use insight_platform_sandbox::{
    ManagedMcpSandboxSecretDeliveryEvidence, ManagedMcpSandboxSessionRequest,
    PreparedManagedMcpSandboxSession, SandboxStopReason, ScopedSecretGrant,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::{fmt, mem};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const MANAGED_MCP_GUEST_PROTOCOL_VERSION: u32 = 1;
pub const MAX_MANAGED_MCP_GUEST_SECRET_BYTES: usize = 16 * 1024;

/// Stable, credential-free fence repeated on every Managed MCP guest frame. A guest cannot
/// substitute a different logical subscription, Executor generation, Provider generation or VM.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedMcpGuestSessionFence {
    pub schema_version: u32,
    pub session_identity_digest: Sha256Digest,
    pub sandbox_job_id: ResourceId,
    pub request_digest: Sha256Digest,
    pub worker_process_generation_id: ResourceId,
    pub provider_process_generation_id: ResourceId,
    pub lease_generation: u64,
    pub sandbox_identity_digest: Sha256Digest,
}

impl ManagedMcpGuestSessionFence {
    pub fn from_prepared(prepared: &PreparedManagedMcpSandboxSession) -> Self {
        Self {
            schema_version: 1,
            session_identity_digest: prepared.identity.canonical_digest.clone(),
            sandbox_job_id: prepared.identity.sandbox_job_id.clone(),
            request_digest: prepared.request_digest.clone(),
            worker_process_generation_id: prepared.worker_process_generation_id.clone(),
            provider_process_generation_id: prepared.provider_process_generation_id.clone(),
            lease_generation: prepared.lease_generation,
            sandbox_identity_digest: prepared.sandbox_identity_digest.clone(),
        }
    }

    pub fn validate_for(
        &self,
        request: &ManagedMcpSandboxSessionRequest,
        fence: &JobFence,
        prepared: &PreparedManagedMcpSandboxSession,
    ) -> Result<(), MicroVmGuestProtocolError> {
        if self.schema_version != 1
            || self.session_identity_digest != request.identity.canonical_digest
            || self.sandbox_job_id != request.identity.sandbox_job_id
            || self.request_digest != request.request_digest
            || self.worker_process_generation_id != fence.worker_process_generation_id
            || self.provider_process_generation_id != prepared.provider_process_generation_id
            || self.lease_generation != fence.lease_generation
            || self.sandbox_identity_digest != prepared.sandbox_identity_digest
            || self.worker_process_generation_id.kind() != ResourceKind::WorkerProcessGeneration
            || self.provider_process_generation_id.kind() != ResourceKind::WorkerProcessGeneration
            || self.sandbox_job_id.kind() != ResourceKind::Job
            || self.lease_generation == 0
        {
            return Err(MicroVmGuestProtocolError::InvalidEnvelope);
        }
        Ok(())
    }

    fn validate_shape(&self) -> Result<(), MicroVmGuestProtocolError> {
        if self.schema_version != 1
            || self.sandbox_job_id.kind() != ResourceKind::Job
            || self.worker_process_generation_id.kind() != ResourceKind::WorkerProcessGeneration
            || self.provider_process_generation_id.kind() != ResourceKind::WorkerProcessGeneration
            || self.lease_generation == 0
        {
            return Err(MicroVmGuestProtocolError::InvalidEnvelope);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedMcpGuestRuntimeArtifactChunk {
    pub artifact: ArtifactRef,
    pub offset: u64,
    pub content_base64: String,
    pub chunk_digest: Sha256Digest,
    pub final_chunk: bool,
}

impl ManagedMcpGuestRuntimeArtifactChunk {
    fn build(
        artifact: ArtifactRef,
        offset: u64,
        content: &[u8],
        final_chunk: bool,
    ) -> Result<Self, MicroVmGuestProtocolError> {
        let chunk = Self {
            artifact,
            offset,
            content_base64: BASE64_STANDARD.encode(content),
            chunk_digest: bytes_digest(content)?,
            final_chunk,
        };
        chunk.validate()?;
        Ok(chunk)
    }

    pub fn decoded_content(&self) -> Result<Vec<u8>, MicroVmGuestProtocolError> {
        let content = BASE64_STANDARD
            .decode(&self.content_base64)
            .map_err(|_| MicroVmGuestProtocolError::InvalidEnvelope)?;
        if content.is_empty()
            || content.len() > crate::MAX_MICROVM_GUEST_ARTIFACT_CHUNK_BYTES
            || BASE64_STANDARD.encode(&content) != self.content_base64
        {
            return Err(MicroVmGuestProtocolError::InvalidEnvelope);
        }
        Ok(content)
    }

    fn validate(&self) -> Result<(), MicroVmGuestProtocolError> {
        self.artifact
            .validate()
            .map_err(|_| MicroVmGuestProtocolError::InvalidEnvelope)?;
        let content = self.decoded_content()?;
        let length =
            u64::try_from(content.len()).map_err(|_| MicroVmGuestProtocolError::InvalidEnvelope)?;
        let end = self
            .offset
            .checked_add(length)
            .ok_or(MicroVmGuestProtocolError::InvalidEnvelope)?;
        if end > self.artifact.byte_length()
            || self.final_chunk != (end == self.artifact.byte_length())
            || self.chunk_digest != bytes_digest(&content)?
        {
            return Err(MicroVmGuestProtocolError::InvalidEnvelope);
        }
        Ok(())
    }
}

/// Non-secret header immediately followed by one raw length-prefixed Secret payload on the same
/// private vsock. Secret bytes are deliberately excluded from JSON/base64, envelope digests and
/// all durable evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedMcpGuestSecretHeader {
    pub secret_binding_id: ResourceId,
    pub purpose: SecretPurpose,
    pub resolved_binding_generation: u64,
    pub grant_digest: Sha256Digest,
    pub delivery_evidence_digest: Sha256Digest,
}

impl ManagedMcpGuestSecretHeader {
    pub fn from_delivery(
        grant: &ScopedSecretGrant,
        evidence: &ManagedMcpSandboxSecretDeliveryEvidence,
    ) -> Result<Self, MicroVmGuestProtocolError> {
        if evidence.secret_binding_id != grant.secret_binding.secret_binding_id
            || evidence.resolved_binding_generation != grant.resolved_binding_generation
        {
            return Err(MicroVmGuestProtocolError::InvalidEnvelope);
        }
        let value = Self {
            secret_binding_id: grant.secret_binding.secret_binding_id.clone(),
            purpose: grant.secret_binding.purpose.clone(),
            resolved_binding_generation: grant.resolved_binding_generation,
            grant_digest: grant.grant_digest.clone(),
            delivery_evidence_digest: evidence.evidence_digest.clone(),
        };
        value.validate_for(grant)?;
        Ok(value)
    }

    fn validate_for(&self, grant: &ScopedSecretGrant) -> Result<(), MicroVmGuestProtocolError> {
        if self.secret_binding_id != grant.secret_binding.secret_binding_id
            || self.purpose != grant.secret_binding.purpose
            || self.resolved_binding_generation != grant.resolved_binding_generation
            || self.grant_digest != grant.grant_digest
            || self.secret_binding_id.kind() != ResourceKind::SecretBinding
            || self.resolved_binding_generation == 0
        {
            return Err(MicroVmGuestProtocolError::InvalidEnvelope);
        }
        Ok(())
    }

    fn validate_shape(&self) -> Result<(), MicroVmGuestProtocolError> {
        if self.secret_binding_id.kind() != ResourceKind::SecretBinding
            || self.resolved_binding_generation == 0
        {
            return Err(MicroVmGuestProtocolError::InvalidEnvelope);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "payload",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum ManagedMcpGuestCommand {
    MaterializeRuntimeArtifact(Box<ManagedMcpGuestRuntimeArtifactChunk>),
    PrepareSecret(ManagedMcpGuestSecretHeader),
    Initialize(Box<ManagedMcpSandboxSessionRequest>),
    Activate {
        activation_binding_digest: Sha256Digest,
    },
    Cancel {
        reason: SandboxStopReason,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedMcpGuestCommandEnvelope {
    pub protocol_version: u32,
    pub sequence: u64,
    pub fence: ManagedMcpGuestSessionFence,
    pub command: ManagedMcpGuestCommand,
    pub envelope_digest: Sha256Digest,
}

impl ManagedMcpGuestCommandEnvelope {
    pub fn materialize_runtime_artifact(
        request: &ManagedMcpSandboxSessionRequest,
        fence: &JobFence,
        prepared: &PreparedManagedMcpSandboxSession,
        sequence: u64,
        offset: u64,
        content: &[u8],
        final_chunk: bool,
    ) -> Result<Self, MicroVmGuestProtocolError> {
        Self::build(
            request,
            fence,
            prepared,
            sequence,
            ManagedMcpGuestCommand::MaterializeRuntimeArtifact(Box::new(
                ManagedMcpGuestRuntimeArtifactChunk::build(
                    request.package.runtime_bundle_artifact.clone(),
                    offset,
                    content,
                    final_chunk,
                )?,
            )),
        )
    }

    pub fn prepare_secret(
        request: &ManagedMcpSandboxSessionRequest,
        fence: &JobFence,
        prepared: &PreparedManagedMcpSandboxSession,
        sequence: u64,
        header: ManagedMcpGuestSecretHeader,
    ) -> Result<Self, MicroVmGuestProtocolError> {
        Self::build(
            request,
            fence,
            prepared,
            sequence,
            ManagedMcpGuestCommand::PrepareSecret(header),
        )
    }

    pub fn initialize(
        request: &ManagedMcpSandboxSessionRequest,
        fence: &JobFence,
        prepared: &PreparedManagedMcpSandboxSession,
        sequence: u64,
    ) -> Result<Self, MicroVmGuestProtocolError> {
        Self::build(
            request,
            fence,
            prepared,
            sequence,
            ManagedMcpGuestCommand::Initialize(Box::new(request.clone())),
        )
    }

    pub fn activate(
        request: &ManagedMcpSandboxSessionRequest,
        fence: &JobFence,
        prepared: &PreparedManagedMcpSandboxSession,
        sequence: u64,
        activation_binding_digest: Sha256Digest,
    ) -> Result<Self, MicroVmGuestProtocolError> {
        Self::build(
            request,
            fence,
            prepared,
            sequence,
            ManagedMcpGuestCommand::Activate {
                activation_binding_digest,
            },
        )
    }

    fn build(
        request: &ManagedMcpSandboxSessionRequest,
        fence: &JobFence,
        prepared: &PreparedManagedMcpSandboxSession,
        sequence: u64,
        command: ManagedMcpGuestCommand,
    ) -> Result<Self, MicroVmGuestProtocolError> {
        let mut value = Self {
            protocol_version: MANAGED_MCP_GUEST_PROTOCOL_VERSION,
            sequence,
            fence: ManagedMcpGuestSessionFence::from_prepared(prepared),
            command,
            envelope_digest: placeholder_digest(),
        };
        value.envelope_digest = digest_without_field(&value, "envelope_digest")?;
        value.validate_for(request, fence, prepared, sequence)?;
        Ok(value)
    }

    pub fn validate_for(
        &self,
        request: &ManagedMcpSandboxSessionRequest,
        fence: &JobFence,
        prepared: &PreparedManagedMcpSandboxSession,
        expected_sequence: u64,
    ) -> Result<(), MicroVmGuestProtocolError> {
        self.fence.validate_for(request, fence, prepared)?;
        let command_valid = match &self.command {
            ManagedMcpGuestCommand::MaterializeRuntimeArtifact(chunk) => {
                chunk.validate().is_ok()
                    && chunk.artifact == request.package.runtime_bundle_artifact
            }
            ManagedMcpGuestCommand::PrepareSecret(header) => request
                .secret_grants
                .iter()
                .any(|grant| header.validate_for(grant).is_ok()),
            ManagedMcpGuestCommand::Initialize(candidate) => candidate.as_ref() == request,
            ManagedMcpGuestCommand::Activate { .. } | ManagedMcpGuestCommand::Cancel { .. } => {
                self.sequence > 1
            }
        };
        if self.protocol_version != MANAGED_MCP_GUEST_PROTOCOL_VERSION
            || self.sequence == 0
            || self.sequence != expected_sequence
            || !command_valid
            || digest_without_field(self, "envelope_digest")? != self.envelope_digest
        {
            return Err(MicroVmGuestProtocolError::InvalidEnvelope);
        }
        Ok(())
    }

    fn validate_secret_shape(&self) -> Result<(), MicroVmGuestProtocolError> {
        self.fence.validate_shape()?;
        let ManagedMcpGuestCommand::PrepareSecret(header) = &self.command else {
            return Err(MicroVmGuestProtocolError::InvalidEnvelope);
        };
        header.validate_shape()?;
        if self.protocol_version != MANAGED_MCP_GUEST_PROTOCOL_VERSION
            || self.sequence == 0
            || digest_without_field(self, "envelope_digest")? != self.envelope_digest
        {
            return Err(MicroVmGuestProtocolError::InvalidEnvelope);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "payload",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum ManagedMcpGuestEvent {
    Ready {
        guest_agent_digest: Sha256Digest,
        runtime_digest: Sha256Digest,
    },
    SecretAccepted {
        secret_binding_id: ResourceId,
        resolved_binding_generation: u64,
        delivery_evidence_digest: Sha256Digest,
        injection_binding_digest: Sha256Digest,
    },
    Initialized {
        established_at: DateTime<Utc>,
        expires_at: DateTime<Utc>,
        protocol_evidence_digest: Sha256Digest,
        activation_binding_digest: Sha256Digest,
    },
    Activated {
        activated_at: DateTime<Utc>,
        activation_evidence_digest: Sha256Digest,
    },
    CancelAcknowledged,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedMcpGuestEventEnvelope {
    pub protocol_version: u32,
    pub sequence: u64,
    pub fence: ManagedMcpGuestSessionFence,
    pub event: ManagedMcpGuestEvent,
    pub envelope_digest: Sha256Digest,
}

impl ManagedMcpGuestEventEnvelope {
    pub fn seal(mut self) -> Result<Self, MicroVmGuestProtocolError> {
        self.envelope_digest = digest_without_field(&self, "envelope_digest")?;
        Ok(self)
    }

    pub fn validate_for(
        &self,
        request: &ManagedMcpSandboxSessionRequest,
        fence: &JobFence,
        prepared: &PreparedManagedMcpSandboxSession,
        expected_sequence: u64,
        now: DateTime<Utc>,
    ) -> Result<(), MicroVmGuestProtocolError> {
        self.fence.validate_for(request, fence, prepared)?;
        let event_valid = match &self.event {
            ManagedMcpGuestEvent::Ready {
                guest_agent_digest,
                runtime_digest,
            } => {
                guest_agent_digest == &request.runtime.guest_agent_digest
                    && runtime_digest == &request.runtime.image_or_module_digest
            }
            ManagedMcpGuestEvent::SecretAccepted {
                secret_binding_id,
                resolved_binding_generation,
                ..
            } => request.secret_grants.iter().any(|grant| {
                secret_binding_id == &grant.secret_binding.secret_binding_id
                    && *resolved_binding_generation == grant.resolved_binding_generation
            }),
            ManagedMcpGuestEvent::Initialized {
                established_at,
                expires_at,
                ..
            } => {
                let maximum = i64::try_from(
                    request
                        .mcp_contract
                        .server
                        .limits
                        .maximum_session_milliseconds,
                )
                .ok()
                .and_then(|milliseconds| {
                    established_at.checked_add_signed(chrono::Duration::milliseconds(milliseconds))
                });
                *expires_at > *established_at
                    && *expires_at <= request.deadline
                    && maximum.is_some_and(|maximum| *expires_at <= maximum)
            }
            ManagedMcpGuestEvent::Activated { activated_at, .. } => {
                *activated_at <= now && *activated_at < request.deadline
            }
            ManagedMcpGuestEvent::CancelAcknowledged => self.sequence > 1,
        };
        if self.protocol_version != MANAGED_MCP_GUEST_PROTOCOL_VERSION
            || self.sequence != expected_sequence
            || self.sequence == 0
            || !event_valid
            || digest_without_field(self, "envelope_digest")? != self.envelope_digest
        {
            return Err(MicroVmGuestProtocolError::InvalidEnvelope);
        }
        Ok(())
    }
}

/// Guest-side sensitive material. Debug is redacted and Drop scrubs any bytes not explicitly
/// transferred into the guest agent's memfd/tmpfs injection primitive.
pub struct SensitiveManagedMcpGuestSecret {
    pub envelope: ManagedMcpGuestCommandEnvelope,
    material: Vec<u8>,
}

impl SensitiveManagedMcpGuestSecret {
    pub fn into_material(mut self) -> Vec<u8> {
        mem::take(&mut self.material)
    }

    pub fn byte_length(&self) -> usize {
        self.material.len()
    }
}

impl fmt::Debug for SensitiveManagedMcpGuestSecret {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SensitiveManagedMcpGuestSecret")
            .field("envelope", &self.envelope)
            .field("material", &"[REDACTED]")
            .field("byte_length", &self.material.len())
            .finish()
    }
}

impl Drop for SensitiveManagedMcpGuestSecret {
    fn drop(&mut self) {
        self.material.fill(0);
    }
}

/// Writes a canonical credential-free header followed by a raw bounded payload. The caller-owned
/// buffer is scrubbed whether the transport succeeds or fails.
pub async fn write_managed_mcp_guest_secret<W>(
    writer: &mut W,
    envelope: &ManagedMcpGuestCommandEnvelope,
    material: &mut [u8],
) -> Result<(), MicroVmGuestProtocolError>
where
    W: AsyncWrite + Unpin,
{
    let result = async {
        envelope.validate_secret_shape()?;
        if material.is_empty() || material.len() > MAX_MANAGED_MCP_GUEST_SECRET_BYTES {
            return Err(MicroVmGuestProtocolError::FrameTooLarge);
        }
        write_guest_frame(writer, envelope).await?;
        let length =
            u32::try_from(material.len()).map_err(|_| MicroVmGuestProtocolError::FrameTooLarge)?;
        writer
            .write_all(&length.to_be_bytes())
            .await
            .map_err(|_| MicroVmGuestProtocolError::Transport)?;
        writer
            .write_all(material)
            .await
            .map_err(|_| MicroVmGuestProtocolError::Transport)?;
        writer
            .flush()
            .await
            .map_err(|_| MicroVmGuestProtocolError::Transport)
    }
    .await;
    material.fill(0);
    result
}

pub async fn read_managed_mcp_guest_secret<R>(
    reader: &mut R,
) -> Result<SensitiveManagedMcpGuestSecret, MicroVmGuestProtocolError>
where
    R: AsyncRead + Unpin,
{
    let envelope: ManagedMcpGuestCommandEnvelope = read_guest_frame(reader).await?;
    envelope.validate_secret_shape()?;
    let mut length = [0_u8; 4];
    reader
        .read_exact(&mut length)
        .await
        .map_err(|_| MicroVmGuestProtocolError::Transport)?;
    let length = usize::try_from(u32::from_be_bytes(length))
        .map_err(|_| MicroVmGuestProtocolError::FrameTooLarge)?;
    if length == 0 || length > MAX_MANAGED_MCP_GUEST_SECRET_BYTES {
        return Err(MicroVmGuestProtocolError::FrameTooLarge);
    }
    let mut material = vec![0_u8; length];
    if reader.read_exact(&mut material).await.is_err() {
        material.fill(0);
        return Err(MicroVmGuestProtocolError::Transport);
    }
    Ok(SensitiveManagedMcpGuestSecret { envelope, material })
}

fn digest_without_field<T: Serialize>(
    value: &T,
    field: &str,
) -> Result<Sha256Digest, MicroVmGuestProtocolError> {
    let mut value =
        serde_json::to_value(value).map_err(|_| MicroVmGuestProtocolError::Canonicalization)?;
    let serde_json::Value::Object(object) = &mut value else {
        return Err(MicroVmGuestProtocolError::Canonicalization);
    };
    object.remove(field);
    canonical_digest(&value)
        .map_err(|_| MicroVmGuestProtocolError::Canonicalization)?
        .parse()
        .map_err(|_| MicroVmGuestProtocolError::Canonicalization)
}

fn bytes_digest(value: &[u8]) -> Result<Sha256Digest, MicroVmGuestProtocolError> {
    let encoded = Sha256::digest(value)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!("sha256:{encoded}")
        .parse()
        .map_err(|_| MicroVmGuestProtocolError::Canonicalization)
}

fn placeholder_digest() -> Sha256Digest {
    "sha256:0000000000000000000000000000000000000000000000000000000000000000"
        .parse()
        .expect("static digest is valid")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(kind: ResourceKind, suffix: u16) -> ResourceId {
        format!(
            "{}_0198f1e8-32e4-75e1-a9e8-d95ca0f5{suffix:04x}",
            kind.descriptor().prefix
        )
        .parse()
        .unwrap()
    }

    fn sha(character: char) -> Sha256Digest {
        format!("sha256:{}", character.to_string().repeat(64))
            .parse()
            .unwrap()
    }

    fn secret_envelope() -> ManagedMcpGuestCommandEnvelope {
        let mut envelope = ManagedMcpGuestCommandEnvelope {
            protocol_version: MANAGED_MCP_GUEST_PROTOCOL_VERSION,
            sequence: 3,
            fence: ManagedMcpGuestSessionFence {
                schema_version: 1,
                session_identity_digest: sha('a'),
                sandbox_job_id: id(ResourceKind::Job, 1),
                request_digest: sha('b'),
                worker_process_generation_id: id(ResourceKind::WorkerProcessGeneration, 2),
                provider_process_generation_id: id(ResourceKind::WorkerProcessGeneration, 3),
                lease_generation: 4,
                sandbox_identity_digest: sha('c'),
            },
            command: ManagedMcpGuestCommand::PrepareSecret(ManagedMcpGuestSecretHeader {
                secret_binding_id: id(ResourceKind::SecretBinding, 4),
                purpose: "mcp.oauth.access_token".parse().unwrap(),
                resolved_binding_generation: 5,
                grant_digest: sha('d'),
                delivery_evidence_digest: sha('e'),
            }),
            envelope_digest: placeholder_digest(),
        };
        envelope.envelope_digest = digest_without_field(&envelope, "envelope_digest").unwrap();
        envelope
    }

    #[tokio::test]
    async fn secret_bytes_are_raw_bounded_and_scrubbed_after_delivery() {
        let envelope = secret_envelope();
        let canary = b"managed-secret-canary".to_vec();
        let mut material = canary.clone();
        let (mut writer, mut reader) = tokio::io::duplex(64 * 1024);
        let send = tokio::spawn(async move {
            write_managed_mcp_guest_secret(&mut writer, &envelope, &mut material)
                .await
                .unwrap();
            material
        });
        let delivered = read_managed_mcp_guest_secret(&mut reader).await.unwrap();
        assert_eq!(delivered.byte_length(), canary.len());
        assert!(!format!("{delivered:?}").contains("managed-secret-canary"));
        assert_eq!(delivered.into_material(), canary);
        assert!(send.await.unwrap().iter().all(|byte| *byte == 0));
    }

    #[tokio::test]
    async fn oversized_secret_is_rejected_and_scrubbed_before_any_payload_write() {
        let envelope = secret_envelope();
        let mut material = vec![7_u8; MAX_MANAGED_MCP_GUEST_SECRET_BYTES + 1];
        let (mut writer, _reader) = tokio::io::duplex(64);
        assert_eq!(
            write_managed_mcp_guest_secret(&mut writer, &envelope, &mut material).await,
            Err(MicroVmGuestProtocolError::FrameTooLarge)
        );
        assert!(material.iter().all(|byte| *byte == 0));
    }
}
