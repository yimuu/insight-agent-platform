//! Physical dependency initialization evidence, owned by the one-shot deployment process.
//! A successful observation is required; a journal never replaces current provider verification.
use crate::{
    installation::{
        InstallationError, InstallationIdentityV1, InstallationInputV1, INSTALLATION_LIMITS,
    },
    openbao::{BaoClientConfigV1, KvV2BindingV1, TransitBindingV1},
};
use insight_platform_contracts::{canonical_digest, parse_strict_json, Sha256Digest};
use serde::{Deserialize, Serialize};

pub const PROVIDER_INITIALIZATION_VERSION: u32 = 1;
pub const OPENBAO_AUTH_MOUNT: &str = "insight-cert";
pub const OPENBAO_TRANSIT_MOUNT: &str = "insight-transit";
pub const OPENBAO_KV_MOUNT: &str = "insight-secrets";
pub const OPENBAO_INITIALIZER_ROLE: &str = "insight-initializer";
pub const OPENBAO_ARTIFACT_KEY: &str = "artifact-reference";
pub const OPENBAO_SECRET_KEY: &str = "secret-reference";
pub const OPENBAO_CANARY_PATH: &str = "readiness";

/// These names are deployment ACL scopes, not platform permission or resource authorities.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum OpenBaoInstallationRole {
    Initializer,
    ArtifactGateway,
    ArtifactData,
    ArtifactMaintenance,
    EgressBroker,
}
impl OpenBaoInstallationRole {
    pub const ALL: &'static [Self] = &[
        Self::Initializer,
        Self::ArtifactGateway,
        Self::ArtifactData,
        Self::ArtifactMaintenance,
        Self::EgressBroker,
    ];
    pub const fn name(self) -> &'static str {
        match self {
            Self::Initializer => OPENBAO_INITIALIZER_ROLE,
            Self::ArtifactGateway => "insight-artifact-gateway",
            Self::ArtifactData => "insight-artifact-data",
            Self::ArtifactMaintenance => "insight-artifact-maintenance",
            Self::EgressBroker => "insight-egress-broker",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstallationProviderReadyV1 {
    /// Actual authenticated provider identity. Files refer only to the private initializer leaf.
    pub client: BaoClientConfigV1,
    pub artifact_key: TransitBindingV1,
    pub secret_key: TransitBindingV1,
    pub secrets: KvV2BindingV1,
    pub canary_version: u32,
    pub canary_digest: Sha256Digest,
}
impl InstallationProviderReadyV1 {
    pub fn validate_for(&self, input: &InstallationInputV1) -> Result<(), InstallationError> {
        self.client
            .validate()
            .map_err(|_| InstallationError::ConfigurationDrift)?;
        self.artifact_key
            .validate_for(&self.client)
            .map_err(|_| InstallationError::ConfigurationDrift)?;
        self.secret_key
            .validate_for(&self.client)
            .map_err(|_| InstallationError::ConfigurationDrift)?;
        self.secrets
            .validate_for(&self.client)
            .map_err(|_| InstallationError::ConfigurationDrift)?;
        if self.client.endpoint != input.network.providers.openbao()?.as_str()
            || self.client.auth_mount != OPENBAO_AUTH_MOUNT
            || self.client.auth_role != OPENBAO_INITIALIZER_ROLE
            || self.client.expected_token_policies != [OPENBAO_INITIALIZER_ROLE]
            || self.artifact_key.mount != OPENBAO_TRANSIT_MOUNT
            || self.secret_key.mount != OPENBAO_TRANSIT_MOUNT
            || self.artifact_key.mount_accessor != self.secret_key.mount_accessor
            || self.artifact_key.name != OPENBAO_ARTIFACT_KEY
            || self.secret_key.name != OPENBAO_SECRET_KEY
            || self.artifact_key.key_version != 1
            || self.secret_key.key_version != 1
            || self.secrets.mount != OPENBAO_KV_MOUNT
            || self.canary_version != 1
        {
            return Err(InstallationError::ConfigurationDrift);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case", deny_unknown_fields)]
pub enum InstallationProviderStateV1 {
    Prepared,
    Requested,
    ProviderReady {
        evidence: Box<InstallationProviderReadyV1>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstallationProviderInitializationV1 {
    pub schema_version: u32,
    pub input_digest: Sha256Digest,
    pub identity_digest: Sha256Digest,
    /// The two pre-frozen initialization/ordinary-serving configurations, not mutable business state.
    pub configuration_digest: Sha256Digest,
    pub state: InstallationProviderStateV1,
}
impl InstallationProviderInitializationV1 {
    pub fn decode(bytes: &[u8]) -> Result<Self, InstallationError> {
        let value = parse_strict_json(bytes, INSTALLATION_LIMITS)
            .map_err(|_| InstallationError::InvalidInput)?;
        let state = value
            .get("state")
            .and_then(serde_json::Value::as_object)
            .ok_or(InstallationError::InvalidInput)?;
        let expected = match state.get("phase").and_then(serde_json::Value::as_str) {
            Some("prepared" | "requested") => 1,
            Some("provider_ready") if state.contains_key("evidence") => 2,
            _ => return Err(InstallationError::InvalidInput),
        };
        // Serde's internally tagged unit variants do not reject extra fields on their own.
        if state.len() != expected {
            return Err(InstallationError::InvalidInput);
        }
        let decoded: Self =
            serde_json::from_value(value).map_err(|_| InstallationError::InvalidInput)?;
        if decoded.schema_version != PROVIDER_INITIALIZATION_VERSION {
            return Err(InstallationError::InvalidInput);
        }
        Ok(decoded)
    }
    pub fn validate_for(
        &self,
        input: &InstallationInputV1,
        identity: &InstallationIdentityV1,
        configuration_digest: &Sha256Digest,
    ) -> Result<(), InstallationError> {
        if self.schema_version != PROVIDER_INITIALIZATION_VERSION
            || self.input_digest != input.digest()?
            || self.identity_digest != identity.digest()?
            || identity.input_digest != self.input_digest
            || &self.configuration_digest != configuration_digest
        {
            return Err(InstallationError::IdentityDrift);
        }
        if let InstallationProviderStateV1::ProviderReady { evidence } = &self.state {
            evidence.validate_for(input)?;
        }
        Ok(())
    }
    /// Caller must fsync the resulting Requested journal before acting on InitializeOnce.
    pub fn request_start(&mut self) -> Result<InstallationProviderStart, InstallationError> {
        match &self.state {
            InstallationProviderStateV1::Prepared => {
                self.state = InstallationProviderStateV1::Requested;
                Ok(InstallationProviderStart::InitializeOnce)
            }
            InstallationProviderStateV1::Requested => {
                Err(InstallationError::ExternalOutcomeUnknown)
            }
            InstallationProviderStateV1::ProviderReady { .. } => {
                Ok(InstallationProviderStart::Serve)
            }
        }
    }
    pub fn complete(
        &mut self,
        input: &InstallationInputV1,
        evidence: InstallationProviderReadyV1,
    ) -> Result<(), InstallationError> {
        evidence.validate_for(input)?;
        match &self.state {
            InstallationProviderStateV1::Requested => {
                self.state = InstallationProviderStateV1::ProviderReady {
                    evidence: Box::new(evidence),
                };
                Ok(())
            }
            InstallationProviderStateV1::ProviderReady { evidence: original }
                if original.as_ref() == &evidence =>
            {
                Ok(())
            }
            _ => Err(InstallationError::Conflict),
        }
    }
    pub fn digest(&self) -> Result<Sha256Digest, InstallationError> {
        canonical_digest(&serde_json::to_value(self).map_err(|_| InstallationError::InvalidInput)?)
            .map_err(|_| InstallationError::InvalidInput)?
            .parse()
            .map_err(|_| InstallationError::InvalidInput)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InstallationProviderStart {
    InitializeOnce,
    Serve,
}

#[cfg(test)]
mod tests {
    use super::*;
    fn journal() -> InstallationProviderInitializationV1 {
        let digest: Sha256Digest = format!("sha256:{}", "a".repeat(64)).parse().unwrap();
        InstallationProviderInitializationV1 {
            schema_version: 1,
            input_digest: digest.clone(),
            identity_digest: digest.clone(),
            configuration_digest: digest,
            state: InstallationProviderStateV1::Prepared,
        }
    }
    #[test]
    fn interrupted_first_start_never_grants_another_initialization() {
        let mut state = journal();
        assert_eq!(
            state.request_start(),
            Ok(InstallationProviderStart::InitializeOnce)
        );
        let bytes = serde_json::to_vec(&state).unwrap();
        let mut resumed = InstallationProviderInitializationV1::decode(&bytes).unwrap();
        assert_eq!(
            resumed.request_start(),
            Err(InstallationError::ExternalOutcomeUnknown)
        );
        assert_eq!(serde_json::to_vec(&resumed).unwrap(), bytes);
    }
    #[test]
    fn unknown_and_incomplete_ready_envelopes_are_rejected() {
        let mut value = serde_json::to_value(journal()).unwrap();
        value["state"] = serde_json::json!({"phase":"provider_ready"});
        assert!(
            InstallationProviderInitializationV1::decode(&serde_json::to_vec(&value).unwrap())
                .is_err()
        );
        value["state"] = serde_json::json!({"phase":"requested", "retry":true});
        assert!(
            InstallationProviderInitializationV1::decode(&serde_json::to_vec(&value).unwrap())
                .is_err()
        );
    }
}
