//! Pure ModelTurn request, stream, tool-intent, and usage decisions.
//!
//! Provider adapters are deliberately absent. Callers supply exact registry facts and
//! PostgreSQL-observed time; repositories persist accepted decisions in caller-owned
//! transactions using the shared Invocation, Job, quota, Event, Receipt, and Outbox authorities.

#![allow(async_fn_in_trait)]

mod state;
mod stream;
mod types;

#[cfg(test)]
mod tests;

pub use insight_platform_contracts::ClosedSchemaDocument;
pub use state::*;
pub use stream::*;
pub use types::*;

use insight_platform_contracts::{HardLimitProfile, JsonLimits};
use std::{error::Error, fmt};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ModelTurnLimits {
    maximum_attempts: u32,
    maximum_request_bytes: usize,
    maximum_response_bytes: usize,
    maximum_delta_bytes: usize,
    maximum_tokens_per_turn: u64,
    maximum_tool_calls: usize,
    maximum_lease_milliseconds: u64,
    inline_value_limits: JsonLimits,
}

impl ModelTurnLimits {
    pub fn from_profile(profile: &HardLimitProfile) -> Result<Self, ModelTurnError> {
        profile
            .validate()
            .map_err(|_| ModelTurnError::InvalidLimits)?;
        let to_usize =
            |value: u64| usize::try_from(value).map_err(|_| ModelTurnError::InvalidLimits);
        let to_u32 = |value: u64| u32::try_from(value).map_err(|_| ModelTurnError::InvalidLimits);
        let maximum_inline_bytes =
            to_usize(profile.run_scheduler.inline_value_bytes.hard_max)?.min(65_536);
        let limits = Self {
            maximum_attempts: to_u32(profile.run_scheduler.attempts_per_work.q1_default)?,
            maximum_request_bytes: to_usize(profile.model_context_mcp.request_bytes.hard_max)?,
            maximum_response_bytes: to_usize(profile.model_context_mcp.response_bytes.hard_max)?,
            maximum_delta_bytes: to_usize(profile.model_context_mcp.delta_bytes.hard_max)?,
            maximum_tokens_per_turn: profile.model_context_mcp.tokens_per_turn.hard_max,
            maximum_tool_calls: to_usize(profile.model_context_mcp.tool_calls_per_turn.hard_max)?,
            maximum_lease_milliseconds: profile.run_scheduler.lease_milliseconds.hard_max,
            inline_value_limits: JsonLimits {
                max_bytes: maximum_inline_bytes,
                max_depth: to_usize(profile.api.json_depth.hard_max)?,
                max_properties_per_object: to_usize(profile.api.json_properties.hard_max)?,
                max_items_per_array: to_usize(profile.api.json_items.hard_max)?,
                max_string_bytes: maximum_inline_bytes,
            },
        };
        if limits.maximum_attempts == 0
            || limits.maximum_request_bytes == 0
            || limits.maximum_response_bytes == 0
            || limits.maximum_delta_bytes == 0
            || limits.maximum_tokens_per_turn == 0
            || limits.maximum_tool_calls == 0
            || limits.maximum_lease_milliseconds == 0
        {
            return Err(ModelTurnError::InvalidLimits);
        }
        Ok(limits)
    }

    #[cfg(test)]
    pub fn contract_fixture() -> Self {
        Self {
            maximum_attempts: 3,
            maximum_request_bytes: 1_048_576,
            maximum_response_bytes: 1_048_576,
            maximum_delta_bytes: 262_144,
            maximum_tokens_per_turn: 16_384,
            maximum_tool_calls: 32,
            maximum_lease_milliseconds: 60_000,
            inline_value_limits: JsonLimits::CONTRACT_FIXTURE,
        }
    }

    pub const fn maximum_attempts(self) -> u32 {
        self.maximum_attempts
    }

    pub const fn maximum_request_bytes(self) -> usize {
        self.maximum_request_bytes
    }

    pub const fn maximum_response_bytes(self) -> usize {
        self.maximum_response_bytes
    }

    pub const fn maximum_delta_bytes(self) -> usize {
        self.maximum_delta_bytes
    }

    pub const fn maximum_tokens_per_turn(self) -> u64 {
        self.maximum_tokens_per_turn
    }

    pub const fn maximum_tool_calls(self) -> usize {
        self.maximum_tool_calls
    }

    pub const fn maximum_lease_milliseconds(self) -> u64 {
        self.maximum_lease_milliseconds
    }

    pub const fn inline_value_limits(self) -> JsonLimits {
        self.inline_value_limits
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelTurnError {
    InvalidLimits,
    InvalidIdentity,
    InvalidAudit,
    InvalidCommand,
    InvalidRun,
    InvalidNode,
    InvalidSelection,
    InvalidDeployment,
    InvalidProvider,
    InvalidProfile,
    InvalidPolicy,
    InvalidPrincipal,
    AdmissionRejected,
    InvalidRequest,
    InvalidRequestValue,
    RequestTooLarge,
    InvalidMessage,
    InvalidArtifact,
    InvalidSchema,
    SchemaValidationFailed,
    InvalidToolProjection,
    InvalidToolResult,
    InvalidToolIntent,
    InvalidResponseContract,
    InvalidResponse,
    InvalidOutputValue,
    ResponseTooLarge,
    InvalidUsage,
    UsageCeilingExceeded,
    InvalidObservation,
    InvalidJob,
    InvalidStream,
    StreamTooLarge,
    StreamAlreadyTerminal,
    InvalidFailure,
    InvalidControl,
    FirstWinnerLost,
    NonCanonicalCollection,
    Canonicalization,
    CounterOverflow,
}

impl fmt::Display for ModelTurnError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidLimits => "ModelTurn limits are invalid",
            Self::InvalidIdentity => "ModelTurn identity is invalid",
            Self::InvalidAudit => "ModelTurn audit is invalid",
            Self::InvalidCommand => "ModelTurn command is invalid",
            Self::InvalidRun => "ModelTurn Run binding is invalid",
            Self::InvalidNode => "ModelTurn NodeExecution binding is invalid",
            Self::InvalidSelection => "ModelTurn model selection is invalid",
            Self::InvalidDeployment => "ModelTurn Deployment closure is invalid",
            Self::InvalidProvider => "Model Provider contract is invalid",
            Self::InvalidProfile => "Model Profile contract is invalid",
            Self::InvalidPolicy => "ModelTurn Policy closure is invalid",
            Self::InvalidPrincipal => "ModelTurn principal snapshot is invalid",
            Self::AdmissionRejected => "ModelTurn admission was rejected",
            Self::InvalidRequest => "canonical model request is invalid",
            Self::InvalidRequestValue => "ModelTurn request Value is invalid",
            Self::RequestTooLarge => "canonical model request exceeds its hard limit",
            Self::InvalidMessage => "canonical model message is invalid",
            Self::InvalidArtifact => "model Artifact input or output is invalid",
            Self::InvalidSchema => "closed model schema is invalid",
            Self::SchemaValidationFailed => "model value fails local schema validation",
            Self::InvalidToolProjection => "model tool projection is invalid",
            Self::InvalidToolResult => "model tool result is not committed or valid",
            Self::InvalidToolIntent => "model tool intent is invalid",
            Self::InvalidResponseContract => "model response contract is invalid",
            Self::InvalidResponse => "normalized model response is invalid",
            Self::InvalidOutputValue => "ModelTurn output Value is invalid",
            Self::ResponseTooLarge => "normalized model response exceeds its hard limit",
            Self::InvalidUsage => "model usage observation is invalid",
            Self::UsageCeilingExceeded => "model usage exceeds the frozen reservation ceiling",
            Self::InvalidObservation => "model provider observation is invalid",
            Self::InvalidJob => "Model Job binding or state is invalid",
            Self::InvalidStream => "model stream frame is invalid or out of order",
            Self::StreamTooLarge => "model stream exceeds its bounded assembler limit",
            Self::StreamAlreadyTerminal => "model stream already received a terminal frame",
            Self::InvalidFailure => "model failure is invalid",
            Self::InvalidControl => "ModelTurn control transition is invalid",
            Self::FirstWinnerLost => "ModelTurn command lost the first-winner race",
            Self::NonCanonicalCollection => "model collection is not canonical",
            Self::Canonicalization => "ModelTurn canonical serialization failed",
            Self::CounterOverflow => "ModelTurn counter overflowed",
        })
    }
}

impl Error for ModelTurnError {}
