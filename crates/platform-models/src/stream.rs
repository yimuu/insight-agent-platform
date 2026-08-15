use crate::{is_code, CanonicalModelResponse, ModelTurnError, ModelTurnLimits};
use insight_platform_contracts::{DataClassification, ResourceId, ResourceKind, Sha256Digest};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum NormalizedModelDelta {
    Text(String),
    ToolArguments {
        call_id: String,
        projected_tool_name: String,
        fragment: String,
    },
    ProviderMetadataDigest(Sha256Digest),
    Terminal(Box<CanonicalModelResponse>),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NormalizedModelFrame {
    pub model_turn_id: ResourceId,
    pub attempt_no: u32,
    pub lease_generation: u64,
    pub transport_sequence: u64,
    pub delta: NormalizedModelDelta,
}

/// Credential-free, lossy observation emitted by a Model Worker after stream-fence validation.
///
/// Only text deltas are eligible for the internal live projection path. This envelope is not the
/// public SSE contract. Incremental tool arguments and Provider metadata remain private until the
/// terminal response has passed complete local validation and the durable ModelTurn transaction
/// wins.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelLiveTextDelta {
    pub schema_version: u32,
    pub tenant_id: ResourceId,
    pub run_id: ResourceId,
    pub model_turn_id: ResourceId,
    pub job_id: ResourceId,
    pub worker_process_generation_id: ResourceId,
    pub attempt_no: u32,
    pub lease_generation: u64,
    pub transport_sequence: u64,
    pub request_digest: Sha256Digest,
    pub classification: DataClassification,
    pub text: String,
}

impl ModelLiveTextDelta {
    pub fn validate(&self, limits: ModelTurnLimits) -> Result<(), ModelTurnError> {
        if self.schema_version != 1
            || self.tenant_id.kind() != ResourceKind::Tenant
            || self.run_id.kind() != ResourceKind::Run
            || self.model_turn_id.kind() != ResourceKind::ModelTurn
            || self.job_id.kind() != ResourceKind::Job
            || self.worker_process_generation_id.kind() != ResourceKind::WorkerProcessGeneration
            || self.attempt_no == 0
            || self.lease_generation == 0
            || self.transport_sequence == 0
            || self.text.is_empty()
            || self.text.contains('\0')
            || serde_json::to_vec(&self.text)
                .map_err(|_| ModelTurnError::Canonicalization)?
                .len()
                > limits.maximum_delta_bytes()
        {
            return Err(ModelTurnError::InvalidStream);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelStreamEvidence {
    pub accepted_delta_count: u32,
    pub accepted_delta_bytes: u64,
    pub terminal_sequence: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ModelStreamAcceptance {
    Live {
        sequence: u64,
        accepted_delta_count: u32,
        accepted_delta_bytes: u64,
    },
    Terminal {
        response: Box<CanonicalModelResponse>,
        evidence: ModelStreamEvidence,
    },
}

#[derive(Debug, Clone)]
pub struct ModelStreamAccumulator {
    model_turn_id: ResourceId,
    attempt_no: u32,
    lease_generation: u64,
    next_sequence: u64,
    accepted_delta_count: u32,
    accepted_delta_bytes: u64,
    terminal: bool,
    limits: ModelTurnLimits,
}

impl ModelStreamAccumulator {
    pub fn new(
        model_turn_id: ResourceId,
        attempt_no: u32,
        lease_generation: u64,
        limits: ModelTurnLimits,
    ) -> Result<Self, ModelTurnError> {
        if model_turn_id.kind() != ResourceKind::ModelTurn
            || attempt_no == 0
            || lease_generation == 0
        {
            return Err(ModelTurnError::InvalidStream);
        }
        Ok(Self {
            model_turn_id,
            attempt_no,
            lease_generation,
            next_sequence: 1,
            accepted_delta_count: 0,
            accepted_delta_bytes: 0,
            terminal: false,
            limits,
        })
    }

    pub fn accept(
        &mut self,
        frame: NormalizedModelFrame,
    ) -> Result<ModelStreamAcceptance, ModelTurnError> {
        if self.terminal {
            return Err(ModelTurnError::StreamAlreadyTerminal);
        }
        if frame.model_turn_id != self.model_turn_id
            || frame.attempt_no != self.attempt_no
            || frame.lease_generation != self.lease_generation
            || frame.transport_sequence != self.next_sequence
        {
            return Err(ModelTurnError::InvalidStream);
        }
        let frame_bytes = serde_json::to_vec(&frame.delta)
            .map_err(|_| ModelTurnError::Canonicalization)?
            .len();
        if frame_bytes > self.limits.maximum_delta_bytes() {
            return Err(ModelTurnError::StreamTooLarge);
        }
        match frame.delta {
            NormalizedModelDelta::Terminal(response) => {
                if response.observation.stream_delta_count != self.accepted_delta_count
                    || response.observation.stream_bytes != self.accepted_delta_bytes
                {
                    return Err(ModelTurnError::InvalidStream);
                }
                self.terminal = true;
                Ok(ModelStreamAcceptance::Terminal {
                    response,
                    evidence: ModelStreamEvidence {
                        accepted_delta_count: self.accepted_delta_count,
                        accepted_delta_bytes: self.accepted_delta_bytes,
                        terminal_sequence: frame.transport_sequence,
                    },
                })
            }
            delta => {
                validate_live_delta(&delta)?;
                self.accepted_delta_count = self
                    .accepted_delta_count
                    .checked_add(1)
                    .ok_or(ModelTurnError::CounterOverflow)?;
                self.accepted_delta_bytes = self
                    .accepted_delta_bytes
                    .checked_add(
                        u64::try_from(frame_bytes).map_err(|_| ModelTurnError::StreamTooLarge)?,
                    )
                    .ok_or(ModelTurnError::StreamTooLarge)?;
                if self.accepted_delta_bytes
                    > u64::try_from(self.limits.maximum_response_bytes())
                        .map_err(|_| ModelTurnError::InvalidLimits)?
                {
                    return Err(ModelTurnError::StreamTooLarge);
                }
                self.next_sequence = self
                    .next_sequence
                    .checked_add(1)
                    .ok_or(ModelTurnError::CounterOverflow)?;
                Ok(ModelStreamAcceptance::Live {
                    sequence: frame.transport_sequence,
                    accepted_delta_count: self.accepted_delta_count,
                    accepted_delta_bytes: self.accepted_delta_bytes,
                })
            }
        }
    }
}

fn validate_live_delta(delta: &NormalizedModelDelta) -> Result<(), ModelTurnError> {
    match delta {
        NormalizedModelDelta::Text(text) if !text.is_empty() && !text.contains('\0') => Ok(()),
        NormalizedModelDelta::ToolArguments {
            call_id,
            projected_tool_name,
            fragment,
        } if is_code(call_id, crate::MAX_MODEL_CALL_ID_BYTES)
            && is_code(projected_tool_name, crate::MAX_MODEL_NAME_BYTES)
            && !fragment.is_empty()
            && !fragment.contains('\0') =>
        {
            Ok(())
        }
        NormalizedModelDelta::ProviderMetadataDigest(_) => Ok(()),
        NormalizedModelDelta::Terminal(_)
        | NormalizedModelDelta::Text(_)
        | NormalizedModelDelta::ToolArguments { .. } => Err(ModelTurnError::InvalidStream),
    }
}
