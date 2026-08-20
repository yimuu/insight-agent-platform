use crate::{
    canonical_digest, canonical_json, parse_strict_json, ArtifactRef, DataClassification,
    JsonLimits, ModelIdentityStability, ModelModality, ResourceId, ResourceKind, SecretPurpose,
    Sha256Digest,
};
use chrono::{DateTime, Utc};
use serde::{de, Deserialize, Deserializer, Serialize};
use serde_json::Value;
use std::{collections::BTreeSet, error::Error, fmt, str::FromStr};

pub const MAX_MODEL_CREDENTIAL_REQUIREMENTS: usize = 16;
pub const MAX_MODEL_REGIONS: usize = 32;
pub const MAX_MODEL_ADAPTER_NAME_BYTES: usize = 192;
pub const MAX_PROVIDER_MODEL_IDENTITY_BYTES: usize = 512;
pub const MAX_MODEL_REQUEST_BYTES: u32 = 16 * 1_048_576;
pub const MAX_MODEL_RESPONSE_BYTES: u32 = 16 * 1_048_576;
pub const MAX_MODEL_MESSAGES: u32 = 4_096;
pub const MAX_MODEL_PARTS: u32 = 16_384;
pub const MAX_MODEL_TOOLS: u32 = 512;
pub const MAX_MODEL_TOOL_CALLS: u32 = 512;
pub const MAX_MODEL_JSON_BYTES: usize = 1_048_576;

pub const MODEL_JSON_LIMITS: JsonLimits = JsonLimits {
    max_bytes: MAX_MODEL_JSON_BYTES,
    max_depth: 32,
    max_properties_per_object: 1_024,
    max_items_per_array: 4_096,
    max_string_bytes: 262_144,
};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct DataRegion(String);

impl DataRegion {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for DataRegion {
    type Err = ModelContractError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.is_empty()
            || value.len() > 32
            || !value.is_ascii()
            || !value.bytes().all(|byte| {
                byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_')
            })
        {
            return Err(ModelContractError::InvalidRegion);
        }
        Ok(Self(value.to_owned()))
    }
}

impl<'de> Deserialize<'de> for DataRegion {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(de::Error::custom)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderTrainingPolicy {
    Prohibited,
    ContractualOptOut,
    ExplicitlyAllowed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClosedJsonValue {
    pub schema_digest: Sha256Digest,
    pub value: Value,
    pub canonical_digest: Sha256Digest,
}

impl ClosedJsonValue {
    pub fn build(schema_digest: Sha256Digest, value: Value) -> Result<Self, ModelContractError> {
        let digest = canonical_digest(&value)
            .map_err(|_| ModelContractError::InvalidJson)?
            .parse()
            .map_err(|_| ModelContractError::InvalidJson)?;
        let document = Self {
            schema_digest,
            value,
            canonical_digest: digest,
        };
        document.validate()?;
        Ok(document)
    }

    pub fn validate(&self) -> Result<(), ModelContractError> {
        let canonical = canonical_json(&self.value).map_err(|_| ModelContractError::InvalidJson)?;
        let reparsed = parse_strict_json(&canonical, MODEL_JSON_LIMITS)
            .map_err(|_| ModelContractError::InvalidJson)?;
        let digest: Sha256Digest = canonical_digest(&reparsed)
            .map_err(|_| ModelContractError::InvalidJson)?
            .parse()
            .map_err(|_| ModelContractError::InvalidJson)?;
        if reparsed != self.value || digest != self.canonical_digest {
            return Err(ModelContractError::InvalidJson);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstalledModelAdapter {
    pub qualified_name: String,
    pub worker_manifest_digest: Sha256Digest,
    pub adapter_contract_digest: Sha256Digest,
}

impl InstalledModelAdapter {
    pub fn validate(&self) -> Result<(), ModelContractError> {
        if self.qualified_name.is_empty()
            || self.qualified_name.len() > MAX_MODEL_ADAPTER_NAME_BYTES
            || !self.qualified_name.is_ascii()
            || !self.qualified_name.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b'/')
            })
        {
            return Err(ModelContractError::InvalidAdapter);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderRequestLimits {
    pub maximum_request_bytes: u32,
    pub maximum_response_bytes: u32,
    pub maximum_messages: u32,
    pub maximum_parts: u32,
    pub maximum_tools: u32,
    pub maximum_parallel_tool_calls: u32,
    pub maximum_stream_delta_bytes: u32,
    pub connect_timeout_milliseconds: u64,
    pub first_byte_timeout_milliseconds: u64,
    pub idle_timeout_milliseconds: u64,
    pub total_timeout_milliseconds: u64,
}

impl ProviderRequestLimits {
    pub fn validate(&self) -> Result<(), ModelContractError> {
        if self.maximum_request_bytes == 0
            || self.maximum_request_bytes > MAX_MODEL_REQUEST_BYTES
            || self.maximum_response_bytes == 0
            || self.maximum_response_bytes > MAX_MODEL_RESPONSE_BYTES
            || self.maximum_messages == 0
            || self.maximum_messages > MAX_MODEL_MESSAGES
            || self.maximum_parts == 0
            || self.maximum_parts > MAX_MODEL_PARTS
            || self.maximum_tools > MAX_MODEL_TOOLS
            || self.maximum_parallel_tool_calls > self.maximum_tools
            || self.maximum_stream_delta_bytes == 0
            || self.maximum_stream_delta_bytes > self.maximum_response_bytes
            || self.connect_timeout_milliseconds == 0
            || self.first_byte_timeout_milliseconds == 0
            || self.idle_timeout_milliseconds == 0
            || self.total_timeout_milliseconds == 0
            || self.connect_timeout_milliseconds >= self.total_timeout_milliseconds
            || self.first_byte_timeout_milliseconds >= self.total_timeout_milliseconds
            || self.idle_timeout_milliseconds >= self.total_timeout_milliseconds
        {
            return Err(ModelContractError::InvalidLimits);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderModelIdentity {
    pub value: String,
    pub stability: ModelIdentityStability,
}

impl ProviderModelIdentity {
    pub fn validate(&self) -> Result<(), ModelContractError> {
        if self.value.is_empty()
            || self.value.len() > MAX_PROVIDER_MODEL_IDENTITY_BYTES
            || self.value.chars().count() > 255
            || self.value.chars().any(char::is_control)
        {
            return Err(ModelContractError::InvalidIdentity);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelModalities {
    pub input: Vec<ModelModality>,
    pub output: Vec<ModelModality>,
}

impl ModelModalities {
    pub fn validate(&self) -> Result<(), ModelContractError> {
        validate_sorted_unique(&self.input, 4)?;
        validate_sorted_unique(&self.output, 4)?;
        if !self.input.contains(&ModelModality::Text) {
            return Err(ModelContractError::InvalidModalities);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextWindowContract {
    pub maximum_context_tokens: u32,
    pub maximum_output_tokens: u32,
    pub tokenizer_contract_digest: Sha256Digest,
    pub estimator_contract_digest: Sha256Digest,
}

impl ContextWindowContract {
    pub fn validate(&self) -> Result<(), ModelContractError> {
        if self.maximum_context_tokens == 0
            || self.maximum_output_tokens == 0
            || self.maximum_output_tokens > self.maximum_context_tokens
        {
            return Err(ModelContractError::InvalidLimits);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelToolContract {
    pub supported: bool,
    pub parallel: bool,
    pub maximum_tools: u32,
    pub maximum_calls_per_turn: u32,
    pub maximum_argument_bytes: u32,
}

impl ModelToolContract {
    pub fn validate(&self) -> Result<(), ModelContractError> {
        let enabled = self.maximum_tools > 0
            && self.maximum_calls_per_turn > 0
            && self.maximum_argument_bytes > 0
            && self.maximum_tools <= MAX_MODEL_TOOLS
            && self.maximum_calls_per_turn <= MAX_MODEL_TOOL_CALLS
            && self.maximum_argument_bytes as usize <= MAX_MODEL_JSON_BYTES;
        if self.supported != enabled || (self.parallel && !self.supported) {
            return Err(ModelContractError::InvalidToolContract);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StructuredOutputContract {
    pub native: bool,
    pub textual_json_fallback: bool,
    pub may_combine_with_tool_intent: bool,
    pub maximum_schema_bytes: u32,
    pub maximum_output_bytes: u32,
}

impl StructuredOutputContract {
    pub fn validate(&self) -> Result<(), ModelContractError> {
        if (!self.native && !self.textual_json_fallback)
            || self.maximum_schema_bytes == 0
            || self.maximum_schema_bytes as usize > MAX_MODEL_JSON_BYTES
            || self.maximum_output_bytes == 0
            || self.maximum_output_bytes > MAX_MODEL_RESPONSE_BYTES
        {
            return Err(ModelContractError::InvalidStructuredOutput);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelUsageContract {
    pub provider_reports_usage: bool,
    pub reports_cached_input_tokens: bool,
    pub reports_reasoning_tokens: bool,
    pub reports_cost: bool,
    pub cost_currency: Option<String>,
    pub estimator_contract_digest: Sha256Digest,
}

impl ModelUsageContract {
    pub fn validate(&self) -> Result<(), ModelContractError> {
        if self.reports_cost != self.cost_currency.is_some()
            || self.cost_currency.as_ref().is_some_and(|currency| {
                currency.len() != 3 || !currency.bytes().all(|byte| byte.is_ascii_uppercase())
            })
        {
            return Err(ModelContractError::InvalidUsageContract);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderDataHandlingContract {
    pub maximum_classification: DataClassification,
    pub allowed_regions: Vec<DataRegion>,
    pub maximum_retention_milliseconds: u64,
    pub training: ProviderTrainingPolicy,
    pub subprocessor_set_digest: Sha256Digest,
}

impl ProviderDataHandlingContract {
    pub fn validate(&self) -> Result<(), ModelContractError> {
        validate_sorted_unique(&self.allowed_regions, MAX_MODEL_REGIONS)?;
        if self.maximum_retention_milliseconds == 0 {
            return Err(ModelContractError::InvalidDataHandling);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelLimits {
    pub maximum_messages: u32,
    pub maximum_parts: u32,
    pub maximum_text_bytes: u32,
    pub maximum_tools: u32,
    pub maximum_parallel_tool_calls: u32,
    pub maximum_rounds: u16,
    pub maximum_input_tokens: u32,
    pub maximum_output_tokens: u32,
}

impl ModelLimits {
    pub fn validate(
        &self,
        context: &ContextWindowContract,
        tools: &ModelToolContract,
    ) -> Result<(), ModelContractError> {
        if self.maximum_messages == 0
            || self.maximum_messages > MAX_MODEL_MESSAGES
            || self.maximum_parts == 0
            || self.maximum_parts > MAX_MODEL_PARTS
            || self.maximum_text_bytes == 0
            || self.maximum_tools > tools.maximum_tools
            || self.maximum_parallel_tool_calls > self.maximum_tools
            || self.maximum_rounds == 0
            || self.maximum_input_tokens == 0
            || self.maximum_input_tokens > context.maximum_context_tokens
            || self.maximum_output_tokens == 0
            || self.maximum_output_tokens > context.maximum_output_tokens
        {
            return Err(ModelContractError::InvalidLimits);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelCatalogEvidence {
    pub artifact: ArtifactRef,
    pub source_digest: Sha256Digest,
    pub adapter_contract_digest: Sha256Digest,
    pub observed_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

impl ModelCatalogEvidence {
    pub fn validate(&self) -> Result<(), ModelContractError> {
        self.artifact
            .validate()
            .map_err(|_| ModelContractError::InvalidEvidence)?;
        if self.expires_at <= self.observed_at {
            return Err(ModelContractError::InvalidEvidence);
        }
        Ok(())
    }
}

pub fn validate_model_provider_contract(
    adapter: &InstalledModelAdapter,
    credential_requirements: &[SecretPurpose],
    limits: &ProviderRequestLimits,
) -> Result<(), ModelContractError> {
    adapter.validate()?;
    limits.validate()?;
    validate_sorted_unique(credential_requirements, MAX_MODEL_CREDENTIAL_REQUIREMENTS)
}

#[allow(clippy::too_many_arguments)]
pub fn validate_model_profile_contract(
    provider_revision: &ResourceId,
    identity: &ProviderModelIdentity,
    modalities: &ModelModalities,
    context: &ContextWindowContract,
    tools: &ModelToolContract,
    structured_output: &StructuredOutputContract,
    usage: &ModelUsageContract,
    data_handling: &ProviderDataHandlingContract,
    limits: &ModelLimits,
    catalog_evidence: &ModelCatalogEvidence,
) -> Result<(), ModelContractError> {
    if provider_revision.kind() != ResourceKind::ModelProviderRevision {
        return Err(ModelContractError::WrongResourceKind);
    }
    identity.validate()?;
    modalities.validate()?;
    context.validate()?;
    tools.validate()?;
    structured_output.validate()?;
    usage.validate()?;
    data_handling.validate()?;
    limits.validate(context, tools)?;
    catalog_evidence.validate()
}

fn validate_sorted_unique<T: Ord>(values: &[T], maximum: usize) -> Result<(), ModelContractError> {
    if values.is_empty() || values.len() > maximum {
        return Err(ModelContractError::UnboundedCollection);
    }
    let unique = values.iter().collect::<BTreeSet<_>>();
    if unique.len() != values.len() || !values.windows(2).all(|pair| pair[0] < pair[1]) {
        return Err(ModelContractError::NonCanonicalCollection);
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ModelContractError {
    WrongResourceKind,
    InvalidAdapter,
    InvalidRegion,
    InvalidIdentity,
    InvalidModalities,
    InvalidLimits,
    InvalidToolContract,
    InvalidStructuredOutput,
    InvalidUsageContract,
    InvalidDataHandling,
    InvalidEvidence,
    InvalidJson,
    UnboundedCollection,
    NonCanonicalCollection,
}

impl fmt::Display for ModelContractError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::WrongResourceKind => "model reference has the wrong resource kind",
            Self::InvalidAdapter => "installed model adapter is invalid",
            Self::InvalidRegion => "model data region is invalid",
            Self::InvalidIdentity => "provider model identity is invalid",
            Self::InvalidModalities => "model modality contract is invalid",
            Self::InvalidLimits => "model limit contract is invalid",
            Self::InvalidToolContract => "model tool contract is invalid",
            Self::InvalidStructuredOutput => "structured output contract is invalid",
            Self::InvalidUsageContract => "model usage contract is invalid",
            Self::InvalidDataHandling => "provider data handling contract is invalid",
            Self::InvalidEvidence => "model catalog evidence is invalid",
            Self::InvalidJson => "closed model JSON value is invalid",
            Self::UnboundedCollection => "model collection is empty or exceeds its hard limit",
            Self::NonCanonicalCollection => "model collection is not sorted and unique",
        })
    }
}

impl Error for ModelContractError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{canonical_digest, ModelIdentityStability, ModelModality};
    use chrono::Duration;
    use serde_json::json;

    fn id(kind: ResourceKind, suffix: u16) -> ResourceId {
        format!(
            "{}_0198f1c8-32e4-75e1-a9e8-d95ca0f4{suffix:04x}",
            kind.descriptor().prefix
        )
        .parse()
        .unwrap()
    }

    fn digest(character: char) -> Sha256Digest {
        format!("sha256:{}", character.to_string().repeat(64))
            .parse()
            .unwrap()
    }

    #[test]
    fn closed_json_value_is_bounded_and_digest_bound() {
        let value = json!({"temperature": 0.2, "max_output_tokens": 128});
        let mut closed = ClosedJsonValue::build(digest('1'), value).unwrap();
        closed.validate().unwrap();
        closed.canonical_digest = digest('2');
        assert_eq!(closed.validate(), Err(ModelContractError::InvalidJson));

        let oversized = json!({"value": "x".repeat(MAX_MODEL_JSON_BYTES)});
        assert_eq!(
            ClosedJsonValue::build(digest('1'), oversized),
            Err(ModelContractError::InvalidJson)
        );
    }

    #[test]
    fn provider_and_profile_contracts_are_closed_and_cross_checked() {
        let adapter = InstalledModelAdapter {
            qualified_name: "openai.responses/v1".to_owned(),
            worker_manifest_digest: digest('1'),
            adapter_contract_digest: digest('2'),
        };
        let credentials = vec!["model_api_key".parse::<SecretPurpose>().unwrap()];
        let request_limits = ProviderRequestLimits {
            maximum_request_bytes: 1_048_576,
            maximum_response_bytes: 1_048_576,
            maximum_messages: 128,
            maximum_parts: 512,
            maximum_tools: 32,
            maximum_parallel_tool_calls: 8,
            maximum_stream_delta_bytes: 65_536,
            connect_timeout_milliseconds: 5_000,
            first_byte_timeout_milliseconds: 30_000,
            idle_timeout_milliseconds: 30_000,
            total_timeout_milliseconds: 120_000,
        };
        validate_model_provider_contract(&adapter, &credentials, &request_limits).unwrap();

        let modalities = ModelModalities {
            input: vec![ModelModality::Text, ModelModality::Image],
            output: vec![ModelModality::Text],
        };
        let context = ContextWindowContract {
            maximum_context_tokens: 128_000,
            maximum_output_tokens: 8_192,
            tokenizer_contract_digest: digest('3'),
            estimator_contract_digest: digest('4'),
        };
        let tools = ModelToolContract {
            supported: true,
            parallel: true,
            maximum_tools: 32,
            maximum_calls_per_turn: 16,
            maximum_argument_bytes: 65_536,
        };
        let structured = StructuredOutputContract {
            native: true,
            textual_json_fallback: false,
            may_combine_with_tool_intent: false,
            maximum_schema_bytes: 65_536,
            maximum_output_bytes: 262_144,
        };
        let usage = ModelUsageContract {
            provider_reports_usage: true,
            reports_cached_input_tokens: true,
            reports_reasoning_tokens: true,
            reports_cost: true,
            cost_currency: Some("USD".to_owned()),
            estimator_contract_digest: digest('5'),
        };
        let data_handling = ProviderDataHandlingContract {
            maximum_classification: DataClassification::Confidential,
            allowed_regions: vec!["us-east_1".parse().unwrap()],
            maximum_retention_milliseconds: 86_400_000,
            training: ProviderTrainingPolicy::Prohibited,
            subprocessor_set_digest: digest('6'),
        };
        let limits = ModelLimits {
            maximum_messages: 128,
            maximum_parts: 512,
            maximum_text_bytes: 1_048_576,
            maximum_tools: 32,
            maximum_parallel_tool_calls: 8,
            maximum_rounds: 16,
            maximum_input_tokens: 120_000,
            maximum_output_tokens: 8_192,
        };
        let observed_at = Utc::now();
        let evidence = ModelCatalogEvidence {
            artifact: ArtifactRef::new(
                id(ResourceKind::Artifact, 1),
                digest('7'),
                512,
                "application/json",
                DataClassification::Internal,
                Some("catalog.json".to_owned()),
            )
            .unwrap(),
            source_digest: digest('8'),
            adapter_contract_digest: adapter.adapter_contract_digest.clone(),
            observed_at,
            expires_at: observed_at + Duration::hours(1),
        };
        validate_model_profile_contract(
            &id(ResourceKind::ModelProviderRevision, 2),
            &ProviderModelIdentity {
                value: "gpt-exact-2026-08-10".to_owned(),
                stability: ModelIdentityStability::Pinned,
            },
            &modalities,
            &context,
            &tools,
            &structured,
            &usage,
            &data_handling,
            &limits,
            &evidence,
        )
        .unwrap();

        let mut invalid_tools = tools;
        invalid_tools.supported = false;
        assert_eq!(
            invalid_tools.validate(),
            Err(ModelContractError::InvalidToolContract)
        );
        let canonical = canonical_digest(&json!({"model": "exact"})).unwrap();
        assert!(canonical.starts_with("sha256:"));
    }
}
