use crate::MicroVmGuestCollection;
use insight_platform_contracts::{
    canonical_digest, parse_strict_json, JsonLimits, ResourceId, ResourceKind, Sha256Digest,
};
use insight_platform_sandbox::{SandboxExecutionRequest, SandboxStopReason};
use serde::{Deserialize, Serialize};
use std::{error::Error, fmt};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const MICROVM_GUEST_PROTOCOL_VERSION: u32 = 1;
pub const MAX_MICROVM_GUEST_FRAME_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "payload",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum MicroVmGuestCommand {
    Execute(Box<SandboxExecutionRequest>),
    Cancel { reason: SandboxStopReason },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MicroVmGuestCommandEnvelope {
    pub protocol_version: u32,
    pub sequence: u64,
    pub sandbox_job_id: ResourceId,
    pub request_digest: Sha256Digest,
    pub attempt_no: u32,
    pub lease_generation: u64,
    pub command: MicroVmGuestCommand,
    pub envelope_digest: Sha256Digest,
}

impl MicroVmGuestCommandEnvelope {
    pub fn execute(request: SandboxExecutionRequest) -> Result<Self, MicroVmGuestProtocolError> {
        let mut value = Self {
            protocol_version: MICROVM_GUEST_PROTOCOL_VERSION,
            sequence: 1,
            sandbox_job_id: request.sandbox_job_id.clone(),
            request_digest: request.request_digest.clone(),
            attempt_no: request.attempt_no,
            lease_generation: request.lease_generation,
            command: MicroVmGuestCommand::Execute(Box::new(request)),
            envelope_digest: placeholder_digest(),
        };
        value.envelope_digest = digest_without_field(&value, "envelope_digest")?;
        value.validate()?;
        Ok(value)
    }

    pub fn cancel(
        request: &SandboxExecutionRequest,
        sequence: u64,
        reason: SandboxStopReason,
    ) -> Result<Self, MicroVmGuestProtocolError> {
        let mut value = Self {
            protocol_version: MICROVM_GUEST_PROTOCOL_VERSION,
            sequence,
            sandbox_job_id: request.sandbox_job_id.clone(),
            request_digest: request.request_digest.clone(),
            attempt_no: request.attempt_no,
            lease_generation: request.lease_generation,
            command: MicroVmGuestCommand::Cancel { reason },
            envelope_digest: placeholder_digest(),
        };
        value.envelope_digest = digest_without_field(&value, "envelope_digest")?;
        value.validate()?;
        Ok(value)
    }

    pub fn validate(&self) -> Result<(), MicroVmGuestProtocolError> {
        let command_matches = match &self.command {
            MicroVmGuestCommand::Execute(request) => {
                request.sandbox_job_id == self.sandbox_job_id
                    && request.request_digest == self.request_digest
                    && request.attempt_no == self.attempt_no
                    && request.lease_generation == self.lease_generation
            }
            MicroVmGuestCommand::Cancel { .. } => self.sequence > 1,
        };
        if self.protocol_version != MICROVM_GUEST_PROTOCOL_VERSION
            || self.sequence == 0
            || self.sandbox_job_id.kind() != ResourceKind::SandboxJob
            || self.attempt_no == 0
            || self.lease_generation == 0
            || !command_matches
            || digest_without_field(self, "envelope_digest")? != self.envelope_digest
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
pub enum MicroVmGuestEvent {
    Ready {
        guest_agent_digest: Sha256Digest,
        runtime_digest: Sha256Digest,
    },
    Result(Box<MicroVmGuestCollection>),
    CancelAcknowledged,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MicroVmGuestEventEnvelope {
    pub protocol_version: u32,
    pub sequence: u64,
    pub sandbox_job_id: ResourceId,
    pub request_digest: Sha256Digest,
    pub attempt_no: u32,
    pub lease_generation: u64,
    pub event: MicroVmGuestEvent,
    pub envelope_digest: Sha256Digest,
}

impl MicroVmGuestEventEnvelope {
    pub fn seal(mut self) -> Result<Self, MicroVmGuestProtocolError> {
        self.envelope_digest = digest_without_field(&self, "envelope_digest")?;
        Ok(self)
    }

    pub fn validate_for(
        &self,
        request: &SandboxExecutionRequest,
        expected_sequence: u64,
    ) -> Result<(), MicroVmGuestProtocolError> {
        if self.protocol_version != MICROVM_GUEST_PROTOCOL_VERSION
            || self.sequence != expected_sequence
            || self.sandbox_job_id != request.sandbox_job_id
            || self.request_digest != request.request_digest
            || self.attempt_no != request.attempt_no
            || self.lease_generation != request.lease_generation
            || digest_without_field(self, "envelope_digest")? != self.envelope_digest
        {
            return Err(MicroVmGuestProtocolError::InvalidEnvelope);
        }
        match &self.event {
            MicroVmGuestEvent::Ready {
                guest_agent_digest,
                runtime_digest,
            } if guest_agent_digest == &request.runtime.guest_agent_digest
                && runtime_digest == &request.runtime.image_or_module_digest => {}
            MicroVmGuestEvent::Result(_) | MicroVmGuestEvent::CancelAcknowledged => {}
            MicroVmGuestEvent::Ready { .. } => {
                return Err(MicroVmGuestProtocolError::InvalidEnvelope);
            }
        }
        Ok(())
    }
}

pub async fn write_guest_frame<W, T>(
    writer: &mut W,
    value: &T,
) -> Result<(), MicroVmGuestProtocolError>
where
    W: AsyncWrite + Unpin,
    T: Serialize,
{
    let bytes =
        serde_jcs::to_vec(value).map_err(|_| MicroVmGuestProtocolError::Canonicalization)?;
    if bytes.is_empty() || bytes.len() > MAX_MICROVM_GUEST_FRAME_BYTES {
        return Err(MicroVmGuestProtocolError::FrameTooLarge);
    }
    let length =
        u32::try_from(bytes.len()).map_err(|_| MicroVmGuestProtocolError::FrameTooLarge)?;
    writer
        .write_all(&length.to_be_bytes())
        .await
        .map_err(|_| MicroVmGuestProtocolError::Transport)?;
    writer
        .write_all(&bytes)
        .await
        .map_err(|_| MicroVmGuestProtocolError::Transport)?;
    writer
        .flush()
        .await
        .map_err(|_| MicroVmGuestProtocolError::Transport)
}

pub async fn read_guest_frame<R, T>(reader: &mut R) -> Result<T, MicroVmGuestProtocolError>
where
    R: AsyncRead + Unpin,
    T: for<'de> Deserialize<'de> + Serialize,
{
    let mut length = [0_u8; 4];
    reader
        .read_exact(&mut length)
        .await
        .map_err(|_| MicroVmGuestProtocolError::Transport)?;
    let length = usize::try_from(u32::from_be_bytes(length))
        .map_err(|_| MicroVmGuestProtocolError::FrameTooLarge)?;
    if length == 0 || length > MAX_MICROVM_GUEST_FRAME_BYTES {
        return Err(MicroVmGuestProtocolError::FrameTooLarge);
    }
    let mut bytes = vec![0_u8; length];
    reader
        .read_exact(&mut bytes)
        .await
        .map_err(|_| MicroVmGuestProtocolError::Transport)?;
    let parsed = parse_strict_json(
        &bytes,
        JsonLimits {
            max_bytes: MAX_MICROVM_GUEST_FRAME_BYTES,
            max_depth: 128,
            max_items_per_array: 100_000,
            max_properties_per_object: 100_000,
            max_string_bytes: MAX_MICROVM_GUEST_FRAME_BYTES,
        },
    )
    .map_err(|_| MicroVmGuestProtocolError::InvalidEnvelope)?;
    let value: T =
        serde_json::from_value(parsed).map_err(|_| MicroVmGuestProtocolError::InvalidEnvelope)?;
    if serde_jcs::to_vec(&value).map_err(|_| MicroVmGuestProtocolError::Canonicalization)? != bytes
    {
        return Err(MicroVmGuestProtocolError::NonCanonicalFrame);
    }
    Ok(value)
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

fn placeholder_digest() -> Sha256Digest {
    "sha256:0000000000000000000000000000000000000000000000000000000000000000"
        .parse()
        .expect("static digest is valid")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MicroVmGuestProtocolError {
    FrameTooLarge,
    NonCanonicalFrame,
    InvalidEnvelope,
    Canonicalization,
    Transport,
}

impl fmt::Display for MicroVmGuestProtocolError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::FrameTooLarge => "microVM guest frame exceeds the protocol bound",
            Self::NonCanonicalFrame => "microVM guest frame is not canonical JSON",
            Self::InvalidEnvelope => "microVM guest envelope is invalid",
            Self::Canonicalization => "microVM guest envelope cannot be canonicalized",
            Self::Transport => "microVM guest transport failed",
        })
    }
}

impl Error for MicroVmGuestProtocolError {}
