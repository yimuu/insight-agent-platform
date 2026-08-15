use crate::{
    canonical_digest, DataClassification, ResourceContractError, ResourceId, ResourceKind,
    Sha256Digest, UtcTimestamp, MAX_ARTIFACT_BYTES, MAX_SAFE_JSON_INTEGER,
};
use chrono::{DateTime, SecondsFormat, TimeDelta, Utc};
use serde::{Deserialize, Serialize};
use std::{error::Error, fmt, str::FromStr};

pub const MODEL_OUTPUT_ARTIFACT_IO_POLICY_VERSION: u32 = 1;
pub const MODEL_OUTPUT_VERIFIED_MEDIA_TYPE: &str = "application/json";

/// Closed `PolicyKind::ArtifactIo` document for Artifact-backed Model responses.
///
/// Candidate-scoped storage facts and effective HardLimit values are deliberately supplied to
/// [`freeze_model_output_artifact_timing`] instead of being inferred from process configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelOutputArtifactIoPolicyDocument {
    pub schema_version: u32,
    pub staging_grace_seconds: u64,
    pub verified_media_type: String,
    pub classification_ceiling: DataClassification,
    pub maximum_materialized_bytes: u64,
    pub storage_binding_digest: Sha256Digest,
    pub encryption_domain_id: ResourceId,
    pub content_validation_profile_digest: Sha256Digest,
}

impl ModelOutputArtifactIoPolicyDocument {
    pub fn validate(&self) -> Result<(), ResourceContractError> {
        if self.schema_version != MODEL_OUTPUT_ARTIFACT_IO_POLICY_VERSION
            || self.staging_grace_seconds == 0
            || self.staging_grace_seconds > MAX_SAFE_JSON_INTEGER
            || self.verified_media_type != MODEL_OUTPUT_VERIFIED_MEDIA_TYPE
            || self.maximum_materialized_bytes == 0
            || self.maximum_materialized_bytes > MAX_ARTIFACT_BYTES
            || self.encryption_domain_id.kind() != ResourceKind::EncryptionDomain
        {
            return Err(ResourceContractError::InvalidPolicyDocument);
        }
        Ok(())
    }

    pub fn canonical_digest(&self) -> Result<Sha256Digest, ResourceContractError> {
        self.validate()?;
        let value =
            serde_json::to_value(self).map_err(|_| ResourceContractError::Canonicalization)?;
        canonical_digest(&value)
            .map_err(|_| ResourceContractError::Canonicalization)?
            .parse()
            .map_err(|_| ResourceContractError::Canonicalization)
    }
}

/// Admission-time values frozen into the Model output reservation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrozenModelOutputArtifactTiming {
    pub staging_retain_until: UtcTimestamp,
    pub ready_retention_seconds: u64,
    pub required_write_quiescence_seconds: u64,
}

/// Pure validation input for the admission-time retention closure.
///
/// `maximum_put_completion_uncertainty_milliseconds` comes from the exact Candidate storage
/// binding. The remaining effective bounds are already-intersected machine/policy values; this
/// contract does not read a mutable deployment environment.
#[derive(Debug, Clone, Copy)]
pub struct ModelOutputArtifactTimingInput<'a> {
    pub db_now: &'a UtcTimestamp,
    pub attempt_deadline: &'a UtcTimestamp,
    pub artifact_io: &'a ModelOutputArtifactIoPolicyDocument,
    pub maximum_put_completion_uncertainty_milliseconds: u64,
    pub effective_artifact_staging_seconds: u64,
    pub ready_retention_seconds: u64,
    pub minimum_ready_retention_seconds: u64,
    pub effective_ready_retention_seconds: u64,
}

/// Freezes the staging deadline and the relative Ready duration before Provider dispatch.
///
/// It intentionally does not calculate an absolute Ready retention timestamp. That timestamp is
/// owned by the later terminal first-winner transaction and is calculated by
/// [`model_output_ready_retain_until`] from that transaction's database time.
pub fn freeze_model_output_artifact_timing(
    input: ModelOutputArtifactTimingInput<'_>,
) -> Result<FrozenModelOutputArtifactTiming, ModelOutputArtifactTimingError> {
    input
        .artifact_io
        .validate()
        .map_err(|_| timing_error("artifact_io", "policy document is invalid"))?;

    if input.maximum_put_completion_uncertainty_milliseconds == 0 {
        return Err(timing_error(
            "maximum_put_completion_uncertainty_milliseconds",
            "must be positive",
        ));
    }
    let required_write_quiescence_seconds = input
        .maximum_put_completion_uncertainty_milliseconds
        .checked_add(999)
        .map(|milliseconds| milliseconds / 1_000)
        .and_then(|seconds| seconds.checked_add(1))
        .ok_or_else(|| {
            timing_error(
                "maximum_put_completion_uncertainty_milliseconds",
                "write-quiescence conversion overflowed",
            )
        })?;
    if input.artifact_io.staging_grace_seconds < required_write_quiescence_seconds {
        return Err(timing_error(
            "staging_grace_seconds",
            "must strictly clear the Candidate storage write-quiescence boundary",
        ));
    }
    if input.effective_artifact_staging_seconds == 0 {
        return Err(timing_error(
            "effective_artifact_staging_seconds",
            "must be positive",
        ));
    }
    if input.ready_retention_seconds == 0
        || input.ready_retention_seconds < input.minimum_ready_retention_seconds
        || input.ready_retention_seconds > input.effective_ready_retention_seconds
    {
        return Err(timing_error(
            "ready_retention_seconds",
            "must be positive and within the frozen Retention/HardLimit intersection",
        ));
    }

    let db_now = parse_timestamp(input.db_now, "db_now")?;
    let attempt_deadline = parse_timestamp(input.attempt_deadline, "attempt_deadline")?;
    if attempt_deadline <= db_now {
        return Err(timing_error(
            "attempt_deadline",
            "must be later than the admission transaction database time",
        ));
    }
    let staging_retain_until = checked_add_seconds(
        attempt_deadline,
        input.artifact_io.staging_grace_seconds,
        "staging_retain_until",
    )?;
    let maximum_staging_retain_until = checked_add_seconds(
        db_now,
        input.effective_artifact_staging_seconds,
        "effective_artifact_staging_seconds",
    )?;
    if staging_retain_until > maximum_staging_retain_until {
        return Err(timing_error(
            "staging_retain_until",
            "attempt deadline plus staging grace exceeds the effective staging window",
        ));
    }

    Ok(FrozenModelOutputArtifactTiming {
        staging_retain_until: format_timestamp(staging_retain_until, "staging_retain_until")?,
        ready_retention_seconds: input.ready_retention_seconds,
        required_write_quiescence_seconds,
    })
}

/// Calculates the absolute Ready retention time in the terminal first-winner transaction.
pub fn model_output_ready_retain_until(
    terminal_db_now: &UtcTimestamp,
    ready_retention_seconds: u64,
) -> Result<UtcTimestamp, ModelOutputArtifactTimingError> {
    if ready_retention_seconds == 0 {
        return Err(timing_error("ready_retention_seconds", "must be positive"));
    }
    let terminal_db_now = parse_timestamp(terminal_db_now, "terminal_db_now")?;
    format_timestamp(
        checked_add_seconds(
            terminal_db_now,
            ready_retention_seconds,
            "ready_retain_until",
        )?,
        "ready_retain_until",
    )
}

fn parse_timestamp(
    value: &UtcTimestamp,
    field: &'static str,
) -> Result<DateTime<Utc>, ModelOutputArtifactTimingError> {
    DateTime::parse_from_rfc3339(value.as_str())
        .map(|value| value.with_timezone(&Utc))
        .map_err(|_| timing_error(field, "timestamp is invalid"))
}

fn checked_add_seconds(
    value: DateTime<Utc>,
    seconds: u64,
    field: &'static str,
) -> Result<DateTime<Utc>, ModelOutputArtifactTimingError> {
    let seconds = i64::try_from(seconds)
        .map_err(|_| timing_error(field, "duration exceeds the clock representation"))?;
    value
        .checked_add_signed(TimeDelta::seconds(seconds))
        .ok_or_else(|| timing_error(field, "timestamp arithmetic overflowed"))
}

fn format_timestamp(
    value: DateTime<Utc>,
    field: &'static str,
) -> Result<UtcTimestamp, ModelOutputArtifactTimingError> {
    UtcTimestamp::from_str(&value.to_rfc3339_opts(SecondsFormat::Micros, true))
        .map_err(|_| timing_error(field, "canonical timestamp encoding failed"))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelOutputArtifactTimingError {
    pub field: &'static str,
    pub message: &'static str,
}

impl fmt::Display for ModelOutputArtifactTimingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "invalid Model output Artifact timing {}: {}",
            self.field, self.message
        )
    }
}

impl Error for ModelOutputArtifactTimingError {}

fn timing_error(field: &'static str, message: &'static str) -> ModelOutputArtifactTimingError {
    ModelOutputArtifactTimingError { field, message }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn digest(character: char) -> Sha256Digest {
        format!("sha256:{}", character.to_string().repeat(64))
            .parse()
            .unwrap()
    }

    fn policy() -> ModelOutputArtifactIoPolicyDocument {
        ModelOutputArtifactIoPolicyDocument {
            schema_version: MODEL_OUTPUT_ARTIFACT_IO_POLICY_VERSION,
            staging_grace_seconds: 32,
            verified_media_type: MODEL_OUTPUT_VERIFIED_MEDIA_TYPE.to_owned(),
            classification_ceiling: DataClassification::Confidential,
            maximum_materialized_bytes: 16_777_216,
            storage_binding_digest: digest('a'),
            encryption_domain_id: ResourceId::from_uuid_v7(
                ResourceKind::EncryptionDomain,
                Uuid::now_v7(),
            )
            .unwrap(),
            content_validation_profile_digest: digest('b'),
        }
    }

    fn timestamp(value: &str) -> UtcTimestamp {
        value.parse().unwrap()
    }

    fn timing_input<'a>(
        artifact_io: &'a ModelOutputArtifactIoPolicyDocument,
        db_now: &'a UtcTimestamp,
        attempt_deadline: &'a UtcTimestamp,
    ) -> ModelOutputArtifactTimingInput<'a> {
        ModelOutputArtifactTimingInput {
            db_now,
            attempt_deadline,
            artifact_io,
            maximum_put_completion_uncertainty_milliseconds: 30_001,
            effective_artifact_staging_seconds: 600,
            ready_retention_seconds: 2_592_000,
            minimum_ready_retention_seconds: 86_400,
            effective_ready_retention_seconds: 31_557_600,
        }
    }

    #[test]
    fn policy_is_closed_exact_and_canonically_bound() {
        let policy = policy();
        policy.validate().unwrap();
        assert_eq!(policy.canonical_digest(), policy.canonical_digest());

        let mut unknown = serde_json::to_value(&policy).unwrap();
        unknown["unknown"] = serde_json::json!(true);
        assert!(serde_json::from_value::<ModelOutputArtifactIoPolicyDocument>(unknown).is_err());

        let mut invalid = policy.clone();
        invalid.verified_media_type = "application/octet-stream".to_owned();
        assert_eq!(
            invalid.validate(),
            Err(ResourceContractError::InvalidPolicyDocument)
        );

        let mut unsafe_json_integer = policy.clone();
        unsafe_json_integer.staging_grace_seconds = MAX_SAFE_JSON_INTEGER + 1;
        assert_eq!(
            unsafe_json_integer.validate(),
            Err(ResourceContractError::InvalidPolicyDocument)
        );

        let mut wrong_domain = policy;
        wrong_domain.encryption_domain_id =
            ResourceId::from_uuid_v7(ResourceKind::Artifact, Uuid::now_v7()).unwrap();
        assert_eq!(
            wrong_domain.validate(),
            Err(ResourceContractError::InvalidPolicyDocument)
        );
    }

    #[test]
    fn admission_freezes_staging_time_and_relative_ready_duration() {
        let policy = policy();
        let db_now = timestamp("2026-08-15T00:00:00.000000Z");
        let attempt_deadline = timestamp("2026-08-15T00:05:00.000000Z");
        let frozen =
            freeze_model_output_artifact_timing(timing_input(&policy, &db_now, &attempt_deadline))
                .unwrap();

        assert_eq!(frozen.required_write_quiescence_seconds, 32);
        assert_eq!(
            frozen.staging_retain_until.as_str(),
            "2026-08-15T00:05:32.000000Z"
        );
        assert_eq!(frozen.ready_retention_seconds, 2_592_000);

        let terminal_db_now = timestamp("2026-08-15T00:06:00.000000Z");
        assert_eq!(
            model_output_ready_retain_until(&terminal_db_now, frozen.ready_retention_seconds)
                .unwrap()
                .as_str(),
            "2026-09-14T00:06:00.000000Z"
        );
    }

    #[test]
    fn admission_rejects_short_grace_window_and_ready_duration_drift() {
        let mut policy = policy();
        policy.staging_grace_seconds = 31;
        let db_now = timestamp("2026-08-15T00:00:00.000000Z");
        let attempt_deadline = timestamp("2026-08-15T00:05:00.000000Z");
        assert_eq!(
            freeze_model_output_artifact_timing(timing_input(&policy, &db_now, &attempt_deadline))
                .unwrap_err()
                .field,
            "staging_grace_seconds"
        );

        policy.staging_grace_seconds = 32;
        let mut too_long = timing_input(&policy, &db_now, &attempt_deadline);
        too_long.effective_artifact_staging_seconds = 331;
        assert_eq!(
            freeze_model_output_artifact_timing(too_long)
                .unwrap_err()
                .field,
            "staging_retain_until"
        );

        let mut bad_ready = timing_input(&policy, &db_now, &attempt_deadline);
        bad_ready.ready_retention_seconds = bad_ready.minimum_ready_retention_seconds - 1;
        assert_eq!(
            freeze_model_output_artifact_timing(bad_ready)
                .unwrap_err()
                .field,
            "ready_retention_seconds"
        );
    }

    #[test]
    fn timing_arithmetic_fails_closed_on_integer_and_clock_overflow() {
        let policy = policy();
        let db_now = timestamp("2026-08-15T00:00:00.000000Z");
        let attempt_deadline = timestamp("2026-08-15T00:05:00.000000Z");
        let mut overflowing_uncertainty = timing_input(&policy, &db_now, &attempt_deadline);
        overflowing_uncertainty.maximum_put_completion_uncertainty_milliseconds = u64::MAX;
        assert_eq!(
            freeze_model_output_artifact_timing(overflowing_uncertainty)
                .unwrap_err()
                .field,
            "maximum_put_completion_uncertainty_milliseconds"
        );

        let clock_ceiling = timestamp("9999-12-31T23:59:59.999999Z");
        assert_eq!(
            model_output_ready_retain_until(&clock_ceiling, 1)
                .unwrap_err()
                .field,
            "ready_retain_until"
        );
    }
}
