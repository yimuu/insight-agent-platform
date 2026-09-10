//! A projection over the existing quota accounts, never a separate allocation authority.
use crate::{ExactDeploymentRef, ResourceId, ResourceKind, MAX_SAFE_JSON_INTEGER};
use serde::{Deserialize, Serialize};
use std::fmt;

pub const INITIAL_MODEL_CONCURRENCY_LIMIT: u64 = 8;
pub const MAX_MODEL_QUOTA_VALUE: u64 = MAX_SAFE_JSON_INTEGER;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelQuotaLimitsV1 {
    pub requests: u64,
    pub tokens: u64,
    pub cost_microunits: u64,
}
impl ModelQuotaLimitsV1 {
    pub fn validate(&self) -> Result<(), ModelQuotaError> {
        if self.values().iter().any(|v| *v > MAX_MODEL_QUOTA_VALUE) {
            return Err(ModelQuotaError);
        }
        Ok(())
    }
    pub fn values(self) -> [u64; 3] {
        [self.requests, self.tokens, self.cost_microunits]
    }
    pub fn from_values(values: [u64; 3]) -> Result<Self, ModelQuotaError> {
        let result = Self {
            requests: values[0],
            tokens: values[1],
            cost_microunits: values[2],
        };
        result.validate()?;
        Ok(result)
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelQuotaAllocationV1 {
    pub limits: ModelQuotaLimitsV1,
    pub reserved: ModelQuotaLimitsV1,
    pub used: ModelQuotaLimitsV1,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelQuotaCounterV1 {
    pub limit: u64,
    pub reserved: u64,
    pub used: u64,
}
impl ModelQuotaCounterV1 {
    pub fn validate(&self) -> Result<(), ModelQuotaError> {
        if [self.limit, self.reserved, self.used]
            .iter()
            .any(|v| *v > MAX_MODEL_QUOTA_VALUE)
            || self
                .reserved
                .checked_add(self.used)
                .is_none_or(|value| value > self.limit)
        {
            return Err(ModelQuotaError);
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelQuotaViewV1 {
    pub schema_version: u16,
    pub tenant_id: ResourceId,
    pub model_deployment: ExactDeploymentRef,
    #[serde(deserialize_with = "required_allocation")]
    pub allocation: Option<ModelQuotaAllocationV1>,
    pub tenant_concurrency: ModelQuotaCounterV1,
    pub etag: String,
}
fn required_allocation<'de, D: serde::Deserializer<'de>>(
    de: D,
) -> Result<Option<ModelQuotaAllocationV1>, D::Error> {
    Option::deserialize(de)
}
impl ModelQuotaViewV1 {
    pub fn validate(&self) -> Result<(), ModelQuotaError> {
        if self.schema_version != 1 || self.tenant_id.kind() != ResourceKind::Tenant {
            return Err(ModelQuotaError);
        }
        validate_model_quota_target(&self.model_deployment)?;
        validate_model_quota_etag(&self.etag)?;
        self.tenant_concurrency.validate()?;
        if let Some(value) = &self.allocation {
            value.limits.validate()?;
            value.reserved.validate()?;
            value.used.validate()?;
            for ((limit, reserved), used) in value
                .limits
                .values()
                .into_iter()
                .zip(value.reserved.values())
                .zip(value.used.values())
            {
                ModelQuotaCounterV1 {
                    limit,
                    reserved,
                    used,
                }
                .validate()?;
            }
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SetModelQuotaRequestV1 {
    pub schema_version: u16,
    pub model_deployment: ExactDeploymentRef,
    pub limits: ModelQuotaLimitsV1,
}
impl SetModelQuotaRequestV1 {
    pub fn validate(&self) -> Result<(), ModelQuotaError> {
        if self.schema_version != 1 {
            return Err(ModelQuotaError);
        }
        validate_model_quota_target(&self.model_deployment)?;
        self.limits.validate()
    }
}
pub fn validate_model_quota_target(value: &ExactDeploymentRef) -> Result<(), ModelQuotaError> {
    if value.resource_kind != ResourceKind::ModelDeployment || value.validate().is_err() {
        return Err(ModelQuotaError);
    }
    Ok(())
}
pub fn validate_model_quota_etag(value: &str) -> Result<(), ModelQuotaError> {
    let Some(hash) = value
        .strip_prefix("\"model-quota-")
        .and_then(|v| v.strip_suffix('"'))
    else {
        return Err(ModelQuotaError);
    };
    if hash.len() != 64
        || !hash
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(ModelQuotaError);
    }
    Ok(())
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ModelQuotaError;
impl fmt::Display for ModelQuotaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("model quota is invalid")
    }
}
impl std::error::Error for ModelQuotaError {}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn limits_are_finite_nonnegative_safe_integers() {
        for n in [0, 8, MAX_MODEL_QUOTA_VALUE] {
            ModelQuotaLimitsV1 {
                requests: n,
                tokens: n,
                cost_microunits: n,
            }
            .validate()
            .unwrap();
        }
        assert!(ModelQuotaLimitsV1 {
            requests: MAX_MODEL_QUOTA_VALUE + 1,
            tokens: 0,
            cost_microunits: 0
        }
        .validate()
        .is_err());
        for raw in ["-1", "1.0", "true", "null"] {
            let json = format!("{{\"requests\":{raw},\"tokens\":0,\"cost_microunits\":0}}");
            assert!(serde_json::from_str::<ModelQuotaLimitsV1>(&json).is_err());
        }
        assert!(ModelQuotaCounterV1 {
            limit: 2,
            reserved: 2,
            used: 1
        }
        .validate()
        .is_err());
    }
    #[test]
    fn quota_cas_is_strong_and_closed() {
        let valid = format!("\"model-quota-{}\"", "a".repeat(64));
        validate_model_quota_etag(&valid).unwrap();
        for invalid in [
            format!("W/{valid}"),
            "*".into(),
            valid.to_uppercase(),
            format!("{valid}, {valid}"),
        ] {
            assert!(validate_model_quota_etag(&invalid).is_err());
        }
    }
}
