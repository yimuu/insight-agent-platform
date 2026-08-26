use crate::{
    id::{ResourceId, ResourceKind},
    json::{parse_strict_json, JsonLimits, MAX_SAFE_JSON_INTEGER},
    registry::{
        validate_public_event_envelope, ApiProblemCode, DataClassification, EventDurability,
        FailureClass, FailureSource, PlatformFailureCode, PublicRunEventSourceKind,
        PublicRunEventType, Retryability,
    },
    TraceId,
};
use chrono::{DateTime, SecondsFormat, Utc};
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use std::{error::Error, fmt, str::FromStr};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Sha256Digest(String);

impl Sha256Digest {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for Sha256Digest {
    type Err = NominalTypeError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let Some(hex) = value.strip_prefix("sha256:") else {
            return Err(NominalTypeError::InvalidDigest);
        };
        if hex.len() != 64
            || !hex
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(NominalTypeError::InvalidDigest);
        }
        Ok(Self(value.to_owned()))
    }
}

impl fmt::Display for Sha256Digest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Serialize for Sha256Digest {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for Sha256Digest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct UtcTimestamp(String);

impl FromStr for UtcTimestamp {
    type Err = NominalTypeError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let bytes = value.as_bytes();
        let microsecond_shape = bytes.len() >= 8
            && value.ends_with('Z')
            && bytes[bytes.len() - 8] == b'.'
            && bytes[bytes.len() - 7..bytes.len() - 1]
                .iter()
                .all(u8::is_ascii_digit);
        if !microsecond_shape {
            return Err(NominalTypeError::InvalidUtcTimestamp);
        }
        let parsed = DateTime::parse_from_rfc3339(value)
            .map_err(|_| NominalTypeError::InvalidUtcTimestamp)?;
        if parsed.offset().local_minus_utc() != 0 {
            return Err(NominalTypeError::InvalidUtcTimestamp);
        }
        Ok(Self(value.to_owned()))
    }
}

impl UtcTimestamp {
    pub fn from_datetime(value: DateTime<Utc>) -> Self {
        Self(value.to_rfc3339_opts(SecondsFormat::Micros, true))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for UtcTimestamp {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for UtcTimestamp {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(de::Error::custom)
    }
}

pub const MAX_ARTIFACT_BYTES: u64 = 1_073_741_824;
pub const MAX_SAFE_TEXT_BYTES: usize = 16_384;
pub const MAX_FIELD_ERRORS: usize = 256;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DecimalMoney {
    currency: String,
    minor_units: i64,
    scale: u8,
}

impl DecimalMoney {
    pub fn new(
        currency: impl Into<String>,
        minor_units: i64,
        scale: u8,
    ) -> Result<Self, NominalTypeError> {
        let value = Self {
            currency: currency.into(),
            minor_units,
            scale,
        };
        value.validate()?;
        Ok(value)
    }

    pub fn currency(&self) -> &str {
        &self.currency
    }

    pub const fn minor_units(&self) -> i64 {
        self.minor_units
    }

    pub const fn scale(&self) -> u8 {
        self.scale
    }

    pub fn validate(&self) -> Result<(), NominalTypeError> {
        if self.currency.len() != 3
            || !self.currency.bytes().all(|byte| byte.is_ascii_uppercase())
            || self.minor_units.unsigned_abs() > MAX_SAFE_JSON_INTEGER
            || self.scale > 18
        {
            return Err(NominalTypeError::InvalidDecimalMoney);
        }
        Ok(())
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DecimalMoneyWire {
    currency: String,
    minor_units: i64,
    scale: u8,
}

impl<'de> Deserialize<'de> for DecimalMoney {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = DecimalMoneyWire::deserialize(deserializer)?;
        Self::new(wire.currency, wire.minor_units, wire.scale).map_err(de::Error::custom)
    }
}

pub const MAX_OPAQUE_CURSOR_BYTES: usize = 8_192;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct OpaqueListCursor(String);

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct OpaqueRunEventCursor(String);

macro_rules! opaque_cursor {
    ($name:ident) => {
        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self, NominalTypeError> {
                let value = value.into();
                if value.is_empty()
                    || value.len() > MAX_OPAQUE_CURSOR_BYTES
                    || !value.is_ascii()
                    || value.bytes().any(|byte| byte.is_ascii_control())
                {
                    return Err(NominalTypeError::InvalidCursor);
                }
                Ok(Self(value))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                Self::new(String::deserialize(deserializer)?).map_err(de::Error::custom)
            }
        }
    };
}

opaque_cursor!(OpaqueListCursor);
opaque_cursor!(OpaqueRunEventCursor);

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ArtifactRef {
    artifact_id: ResourceId,
    content_digest: Sha256Digest,
    byte_length: u64,
    media_type: String,
    classification: DataClassification,
    display_name: Option<String>,
}

impl ArtifactRef {
    pub fn new(
        artifact_id: ResourceId,
        content_digest: Sha256Digest,
        byte_length: u64,
        media_type: impl Into<String>,
        classification: DataClassification,
        display_name: Option<String>,
    ) -> Result<Self, NominalTypeError> {
        let reference = Self {
            artifact_id,
            content_digest,
            byte_length,
            media_type: media_type.into(),
            classification,
            display_name,
        };
        reference.validate()?;
        Ok(reference)
    }

    pub fn artifact_id(&self) -> &ResourceId {
        &self.artifact_id
    }

    pub fn content_digest(&self) -> &Sha256Digest {
        &self.content_digest
    }

    pub const fn byte_length(&self) -> u64 {
        self.byte_length
    }

    pub fn media_type(&self) -> &str {
        &self.media_type
    }

    pub const fn classification(&self) -> DataClassification {
        self.classification
    }

    pub fn display_name(&self) -> Option<&str> {
        self.display_name.as_deref()
    }

    pub fn validate(&self) -> Result<(), NominalTypeError> {
        if self.artifact_id.kind() != ResourceKind::Artifact {
            return Err(NominalTypeError::WrongResourceKind);
        }
        if self.byte_length > MAX_ARTIFACT_BYTES
            || self.media_type.is_empty()
            || self.media_type.len() > 255
            || !self.media_type.is_ascii()
            || self.media_type.bytes().any(|byte| byte.is_ascii_control())
        {
            return Err(NominalTypeError::InvalidArtifactRef);
        }
        if self.display_name.as_ref().is_some_and(|name| {
            name.is_empty()
                || name.chars().count() > 255
                || name.len() > 1_020
                || name.chars().any(char::is_control)
                || name.contains('/')
                || name.contains('\\')
        }) {
            return Err(NominalTypeError::InvalidArtifactRef);
        }
        Ok(())
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ArtifactRefWire {
    artifact_id: ResourceId,
    content_digest: Sha256Digest,
    byte_length: u64,
    media_type: String,
    classification: DataClassification,
    display_name: Option<String>,
}

impl<'de> Deserialize<'de> for ArtifactRef {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ArtifactRefWire::deserialize(deserializer)?;
        Self::new(
            wire.artifact_id,
            wire.content_digest,
            wire.byte_length,
            wire.media_type,
            wire.classification,
            wire.display_name,
        )
        .map_err(de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ValueRef {
    Inline { value: Value },
    Artifact { artifact: ArtifactRef },
}

impl ValueRef {
    pub fn validate(&self, inline_limits: JsonLimits) -> Result<(), NominalTypeError> {
        match self {
            Self::Inline { value } => {
                let bytes =
                    serde_json::to_vec(value).map_err(|_| NominalTypeError::InvalidInlineValue)?;
                parse_strict_json(&bytes, inline_limits)
                    .map(|_| ())
                    .map_err(|_| NominalTypeError::InvalidInlineValue)
            }
            Self::Artifact { artifact } => artifact.validate(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct DeclaredFailureCode(String);

impl FromStr for DeclaredFailureCode {
    type Err = NominalTypeError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if !crate::registry::is_valid_declared_failure_code(value) {
            return Err(NominalTypeError::InvalidDeclaredFailureCode);
        }
        Ok(Self(value.to_owned()))
    }
}

impl DeclaredFailureCode {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for DeclaredFailureCode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum FailureCode {
    Platform {
        code: PlatformFailureCode,
    },
    Declared {
        interface_revision_id: ResourceId,
        code: DeclaredFailureCode,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Failure {
    pub code: FailureCode,
    pub class: FailureClass,
    pub retryability: Retryability,
    pub safe_message: Option<String>,
    pub details_ref: Option<ArtifactRef>,
    pub source: FailureSource,
}

impl Failure {
    pub fn validate(&self, max_safe_message_bytes: usize) -> Result<(), NominalTypeError> {
        if let FailureCode::Declared {
            interface_revision_id,
            ..
        } = &self.code
        {
            if !matches!(
                interface_revision_id.kind(),
                ResourceKind::AgentInterfaceRevision | ResourceKind::CapabilityInterfaceRevision
            ) {
                return Err(NominalTypeError::WrongResourceKind);
            }
        }
        if self.safe_message.as_ref().is_some_and(|message| {
            message.len() > max_safe_message_bytes || message.chars().any(char::is_control)
        }) {
            return Err(NominalTypeError::UnsafeMessage);
        }
        if let Some(reference) = &self.details_ref {
            reference.validate()?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FieldError {
    pub field: String,
    pub code: String,
    pub safe_message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApiProblem {
    pub type_uri: String,
    pub title: String,
    pub status: u16,
    pub code: ApiProblemCode,
    pub detail: Option<String>,
    pub request_id: ResourceId,
    pub trace_id: TraceId,
    pub retryable: bool,
    pub retry_after_ms: Option<u64>,
    pub field_errors: Vec<FieldError>,
}

impl ApiProblem {
    pub fn validate(
        &self,
        max_safe_text_bytes: usize,
        max_field_errors: usize,
    ) -> Result<(), NominalTypeError> {
        if self.request_id.kind() != ResourceKind::ServerRequest
            || !(400..=599).contains(&self.status)
            || self.title.is_empty()
            || self.title.len() > max_safe_text_bytes
            || self
                .detail
                .as_ref()
                .is_some_and(|value| value.len() > max_safe_text_bytes)
            || self.field_errors.len() > max_field_errors
            || self
                .field_errors
                .iter()
                .any(|field| field.validate(max_safe_text_bytes).is_err())
        {
            return Err(NominalTypeError::InvalidApiProblem);
        }
        Ok(())
    }
}

impl FieldError {
    pub fn validate(&self, max_safe_text_bytes: usize) -> Result<(), NominalTypeError> {
        if self.field.is_empty()
            || self.field.len() > 2_048
            || self.field.chars().any(char::is_control)
            || !crate::registry::is_valid_declared_failure_code(&self.code)
            || self.safe_message.as_ref().is_some_and(|message| {
                message.len() > max_safe_text_bytes || message.chars().any(char::is_control)
            })
        {
            return Err(NominalTypeError::InvalidApiProblem);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DurablePublicRunEventData {
    pub source_kind: PublicRunEventSourceKind,
    pub source_id: ResourceId,
    pub source_projection_version: u64,
    pub safe_summary: Option<String>,
}

pub const MAX_PUBLIC_EVENT_SAFE_SUMMARY_BYTES: usize = 2_048;

impl DurablePublicRunEventData {
    pub fn validate(&self, event_type: PublicRunEventType) -> Result<(), NominalTypeError> {
        if event_type.durable_source_kind() != Some(self.source_kind)
            || self.source_id.kind() != self.source_kind.resource_kind()
            || self.source_projection_version == 0
            || self.source_projection_version > MAX_SAFE_JSON_INTEGER
            || self.safe_summary.as_ref().is_some_and(|summary| {
                summary.is_empty()
                    || summary.len() > MAX_PUBLIC_EVENT_SAFE_SUMMARY_BYTES
                    || summary.chars().any(char::is_control)
            })
        {
            return Err(NominalTypeError::InvalidEventPayload);
        }
        Ok(())
    }

    pub fn from_value(
        event_type: PublicRunEventType,
        value: &Value,
    ) -> Result<Self, NominalTypeError> {
        let data = serde_json::from_value::<Self>(value.clone())
            .map_err(|_| NominalTypeError::InvalidEventPayload)?;
        data.validate(event_type)?;
        Ok(data)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicRunEvent {
    pub event_id: Option<ResourceId>,
    pub run_id: ResourceId,
    pub cursor: Option<OpaqueRunEventCursor>,
    pub sequence: Option<u64>,
    pub schema_version: u32,
    pub trace_id: TraceId,
    pub event_type: PublicRunEventType,
    pub durability: EventDurability,
    pub occurred_at: UtcTimestamp,
    pub data: Value,
}

impl PublicRunEvent {
    pub fn validate(&self) -> Result<(), NominalTypeError> {
        if self.run_id.kind() != ResourceKind::Run
            || self.schema_version == 0
            || self
                .event_id
                .as_ref()
                .is_some_and(|event_id| event_id.kind() != ResourceKind::Event)
        {
            return Err(NominalTypeError::WrongResourceKind);
        }
        validate_public_event_envelope(
            self.event_type,
            self.durability,
            self.event_id.is_some(),
            self.sequence.is_some(),
            self.cursor.is_some(),
        )
        .map_err(|_| NominalTypeError::InvalidEventEnvelope)?;
        if self.durability == EventDurability::Durable {
            DurablePublicRunEventData::from_value(self.event_type, &self.data)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NominalTypeError {
    InvalidDigest,
    InvalidUtcTimestamp,
    InvalidDecimalMoney,
    InvalidArtifactRef,
    InvalidDeclaredFailureCode,
    InvalidCursor,
    WrongResourceKind,
    UnsafeMessage,
    InvalidApiProblem,
    InvalidEventEnvelope,
    InvalidEventPayload,
    InvalidInlineValue,
}

impl fmt::Display for NominalTypeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidDigest => "digest must be sha256:<64 lowercase hex>",
            Self::InvalidUtcTimestamp => {
                "timestamp must be UTC RFC 3339 with six fractional digits"
            }
            Self::InvalidDecimalMoney => "DecimalMoney currency/scale is invalid",
            Self::InvalidArtifactRef => "ArtifactRef is invalid",
            Self::InvalidDeclaredFailureCode => "declared failure code is invalid",
            Self::InvalidCursor => "opaque cursor violates its bounded wire contract",
            Self::WrongResourceKind => "nominal resource ID has the wrong kind",
            Self::UnsafeMessage => "safe message violates its bounded projection contract",
            Self::InvalidApiProblem => "ApiProblem violates its safe envelope contract",
            Self::InvalidEventEnvelope => {
                "PublicRunEvent violates its durability envelope contract"
            }
            Self::InvalidEventPayload => {
                "durable PublicRunEvent data violates its closed source projection contract"
            }
            Self::InvalidInlineValue => "inline ValueRef violates its bounded JSON contract",
        })
    }
}

impl Error for NominalTypeError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nominal_scalars_fail_closed() {
        assert!(
            "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                .parse::<Sha256Digest>()
                .is_ok()
        );
        assert!("SHA256:00".parse::<Sha256Digest>().is_err());
        assert!("2026-08-07T12:34:56.123456Z"
            .parse::<UtcTimestamp>()
            .is_ok());
        assert!("2026-08-07T20:34:56.123456+08:00"
            .parse::<UtcTimestamp>()
            .is_err());
        assert!(OpaqueListCursor::new("").is_err());
        assert!(OpaqueRunEventCursor::new("opaque-token").is_ok());
        assert!(serde_json::from_str::<DeclaredFailureCode>("\"Platform\"").is_err());
    }

    #[test]
    fn money_is_explicit_and_bounded() {
        DecimalMoney {
            currency: "CNY".to_owned(),
            minor_units: 1234,
            scale: 2,
        }
        .validate()
        .unwrap();
        assert!(DecimalMoney {
            currency: "cny".to_owned(),
            minor_units: 1234,
            scale: 2,
        }
        .validate()
        .is_err());
    }

    #[test]
    fn durable_public_event_payload_is_closed_and_source_typed() {
        let data = serde_json::json!({
            "source_kind": "run_control",
            "source_id": "run_018f0000-0000-7000-8000-000000000001",
            "source_projection_version": 2,
            "safe_summary": null
        });
        assert!(
            DurablePublicRunEventData::from_value(PublicRunEventType::RunPaused, &data).is_ok()
        );
        assert!(
            DurablePublicRunEventData::from_value(PublicRunEventType::RunCompleted, &data).is_err()
        );
        assert!(
            DurablePublicRunEventData::from_value(PublicRunEventType::RunSnapshot, &data).is_err()
        );

        let mut with_unknown = data;
        with_unknown["raw_result"] = serde_json::json!({"secret": true});
        assert!(DurablePublicRunEventData::from_value(
            PublicRunEventType::RunPaused,
            &with_unknown
        )
        .is_err());
    }

    #[test]
    fn every_event_type_has_an_explicit_durable_source_decision() {
        let without_durable_projection = PublicRunEventType::ALL
            .iter()
            .filter(|event_type| event_type.durable_source_kind().is_none())
            .map(|event_type| event_type.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            without_durable_projection,
            vec!["run.snapshot", "model.delta", "stream.live_gap"]
        );
    }
}
