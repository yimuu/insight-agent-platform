use insight_platform_contracts::*;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// Convenience mappings supplied by this CLI, not assumptions about a provider's environment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EnvironmentPreset {
    Openai,
    Dashscope,
    Anthropic,
}
impl EnvironmentPreset {
    fn mapping(self) -> ModelEnvironmentMappingV1 {
        let (key, base, model) = match self {
            Self::Openai => ("OPENAI_API_KEY", "OPENAI_BASE_URL", "OPENAI_DEFAULT_MODEL"),
            Self::Dashscope => ("DASHSCOPE_API_KEY", "DASHSCOPE_BASE_URL", "DASHSCOPE_MODEL"),
            Self::Anthropic => ("ANTHROPIC_API_KEY", "ANTHROPIC_BASE_URL", "ANTHROPIC_MODEL"),
        };
        ModelEnvironmentMappingV1 {
            schema_version: 1,
            api_key: key.parse().expect("preset variable"),
            base_url: Some(base.parse().expect("preset variable")),
            model: Some(model.parse().expect("preset variable")),
        }
    }
    fn protocol(self) -> ModelProviderWireProtocol {
        match self {
            Self::Openai | Self::Dashscope => ModelProviderWireProtocol::OpenAiResponses,
            Self::Anthropic => ModelProviderWireProtocol::AnthropicMessages,
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigurationFileV1 {
    pub schema_version: u16,
    pub sources: Vec<SourceInputV1>,
    pub default_model: Option<ResourceAlias>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceInputV1 {
    pub alias: ResourceAlias,
    pub display_name: String,
    pub preset: Option<EnvironmentPreset>,
    pub environment: Option<ModelEnvironmentMappingV1>,
    pub api_key_file: Option<String>,
    pub protocol: Option<ModelProviderWireProtocol>,
    pub base_url: Option<String>,
    pub models: Vec<ModelInputV1>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelInputV1 {
    pub alias: ResourceAlias,
    pub display_name: String,
    pub model: Option<String>,
    pub model_environment_variable: Option<EnvironmentVariableName>,
    pub quota: ModelQuotaLimitsV1,
    #[serde(default = "default_input")]
    pub maximum_input_tokens: u32,
    #[serde(default = "default_output")]
    pub maximum_output_tokens: u32,
}
fn default_input() -> u32 {
    8192
}
fn default_output() -> u32 {
    1024
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedModel {
    pub alias: ResourceAlias,
    pub display_name: String,
    pub model: String,
    pub maximum_input_tokens: u32,
    pub maximum_output_tokens: u32,
    pub quota: ModelQuotaLimitsV1,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedSource {
    pub alias: ResourceAlias,
    pub display_name: String,
    pub credential: CredentialInput,
    pub endpoint: CanonicalHttpEndpoint,
    pub protocol: ModelProviderWireProtocol,
    pub models: Vec<ResolvedModel>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum CredentialInput {
    Environment { variable: EnvironmentVariableName },
    File { path: String },
}
pub struct EnvironmentSnapshot {
    variables: BTreeMap<String, Vec<u8>>,
    files: BTreeMap<String, SensitiveModelApiKey>,
}
impl Drop for EnvironmentSnapshot {
    fn drop(&mut self) {
        for value in self.variables.values_mut() {
            value.fill(0);
        }
    }
}
impl EnvironmentSnapshot {
    pub fn key(&self, input: &CredentialInput) -> Result<SensitiveModelApiKey, String> {
        let bytes = match input {
            CredentialInput::Environment { variable } => self
                .variables
                .get(variable.as_str())
                .ok_or("mapped key is unavailable")?
                .clone(),
            CredentialInput::File { path } => self
                .files
                .get(path)
                .ok_or("mapped key file is unavailable")?
                .expose()
                .to_vec(),
        };
        SensitiveModelApiKey::new(bytes).map_err(|_| {
            "mapped credential must contain a nonempty visible-ASCII API key".to_owned()
        })
    }
    fn text(&self, variable: &EnvironmentVariableName) -> Result<&str, String> {
        std::str::from_utf8(
            self.variables
                .get(variable.as_str())
                .ok_or("mapped input is unavailable")?,
        )
        .map_err(|_| format!("{} must contain UTF-8 input", variable.as_str()))
    }
}
impl ConfigurationFileV1 {
    pub fn resolve(
        &self,
        mut read: impl FnMut(&str) -> Result<String, String>,
        mut read_file: impl FnMut(&str) -> Result<SensitiveModelApiKey, String>,
    ) -> Result<(Vec<ResolvedSource>, EnvironmentSnapshot), String> {
        if self.schema_version != 1 || self.sources.is_empty() || self.sources.len() > 32 {
            return Err("configuration requires 1..32 sources".to_owned());
        }
        let mut source_aliases = BTreeSet::new();
        let mut model_aliases = BTreeSet::new();
        let mut variables = BTreeSet::new();
        let mut mappings = Vec::new();
        let mut files = BTreeSet::new();
        let mut credential_variables = BTreeSet::new();
        let mut public_variables = BTreeSet::new();
        for source in &self.sources {
            if !source_aliases.insert(source.alias.as_str())
                || !valid_name(&source.display_name)
                || source.models.len() > 32
            {
                return Err("source aliases, display names or model count are invalid".to_owned());
            }
            if source.api_key_file.is_some() && source.environment.is_some() {
                return Err(
                    "choose a key file or explicit credential environment mapping, not both"
                        .to_owned(),
                );
            }
            let mapping = source
                .environment
                .clone()
                .or_else(|| source.preset.map(EnvironmentPreset::mapping));
            if let Some(mapping) = &mapping {
                mapping
                    .validate()
                    .map_err(|_| "environment variable mapping is invalid")?;
            }
            let credential = if let Some(path) = &source.api_key_file {
                if path.is_empty() || path.len() > 2048 || path.chars().any(char::is_control) {
                    return Err("credential file path is invalid".to_owned());
                }
                files.insert(path.clone());
                CredentialInput::File { path: path.clone() }
            } else {
                let mapping = mapping
                    .as_ref()
                    .ok_or("choose a key file, preset or explicit environment mapping")?;
                variables.insert(mapping.api_key.as_str().to_owned());
                credential_variables.insert(mapping.api_key.as_str().to_owned());
                CredentialInput::Environment {
                    variable: mapping.api_key.clone(),
                }
            };
            if source.base_url.is_none() {
                let name = mapping
                    .as_ref()
                    .and_then(|m| m.base_url.as_ref())
                    .ok_or("source requires a base URL or mapped variable")?
                    .as_str()
                    .to_owned();
                variables.insert(name.clone());
                public_variables.insert(name);
            }
            for model in &source.models {
                if !model_aliases.insert(model.alias.as_str())
                    || !valid_name(&model.display_name)
                    || model.maximum_input_tokens == 0
                    || model.maximum_input_tokens > 8192
                    || model.maximum_output_tokens == 0
                    || model.maximum_output_tokens > 2048
                    || model.quota.validate().is_err()
                    || model.model.is_some() && model.model_environment_variable.is_some()
                {
                    return Err("model aliases, inputs or token limits are invalid".to_owned());
                }
                if model.model.is_none() {
                    let variable = model
                        .model_environment_variable
                        .as_ref()
                        .or(mapping.as_ref().and_then(|m| m.model.as_ref()))
                        .ok_or("model requires an identity or mapped variable")?;
                    if mapping.as_ref().is_some_and(|m| {
                        variable == &m.api_key || Some(variable) == m.base_url.as_ref()
                    }) {
                        return Err(
                            "model identity variable overlaps credential or endpoint input"
                                .to_owned(),
                        );
                    }
                    variables.insert(variable.as_str().to_owned());
                    public_variables.insert(variable.as_str().to_owned());
                }
            }
            mappings.push((mapping, credential));
        }
        if self
            .default_model
            .as_ref()
            .is_some_and(|alias| !model_aliases.contains(alias.as_str()))
        {
            return Err("default_model must name one configured model alias".to_owned());
        }
        if !credential_variables.is_disjoint(&public_variables) {
            return Err(
                "credential variables must not also supply public model metadata across sources"
                    .to_owned(),
            );
        }
        let mut snapshot = EnvironmentSnapshot {
            variables: BTreeMap::new(),
            files: BTreeMap::new(),
        };
        for name in variables {
            let value = read(&name)
                .map_err(|_| format!("mapped environment variable {name} is unavailable"))?;
            if value.is_empty() || value.len() > 4096 {
                let mut bytes = value.into_bytes();
                bytes.fill(0);
                return Err(format!(
                    "mapped environment variable {name} is outside its size bound"
                ));
            }
            snapshot.variables.insert(name, value.into_bytes());
        }
        for path in files {
            snapshot.files.insert(
                path.clone(),
                read_file(&path).map_err(|_| "mapped credential file is unavailable or invalid")?,
            );
        }
        let mut sources = Vec::new();
        for (source, (mapping, credential)) in self.sources.iter().zip(mappings) {
            snapshot.key(&credential)?;
            let base = match source.base_url.as_deref() {
                Some(value) => value,
                None => snapshot.text(
                    mapping
                        .as_ref()
                        .and_then(|m| m.base_url.as_ref())
                        .ok_or("missing endpoint input")?,
                )?,
            };
            let endpoint =
                normalize_model_base_url(base).map_err(|_| "model base URL is invalid")?;
            let protocol = source
                .protocol
                .or(source.preset.map(EnvironmentPreset::protocol))
                .ok_or("explicit mapping requires an explicit protocol")?;
            let mut models = Vec::new();
            for model in &source.models {
                let identity = match model.model.as_deref() {
                    Some(value) => value,
                    None => snapshot.text(
                        model
                            .model_environment_variable
                            .as_ref()
                            .or(mapping.as_ref().and_then(|m| m.model.as_ref()))
                            .ok_or("missing model input")?,
                    )?,
                };
                ProviderModelIdentity {
                    value: identity.to_owned(),
                    stability: ModelIdentityStability::ExternallyMutable,
                }
                .validate()
                .map_err(|_| "provider model identity is invalid")?;
                models.push(ResolvedModel {
                    alias: model.alias.clone(),
                    display_name: model.display_name.clone(),
                    model: identity.to_owned(),
                    maximum_input_tokens: model.maximum_input_tokens,
                    maximum_output_tokens: model.maximum_output_tokens,
                    quota: model.quota,
                });
            }
            sources.push(ResolvedSource {
                alias: source.alias.clone(),
                display_name: source.display_name.clone(),
                credential,
                endpoint,
                protocol,
                models,
            });
        }
        Ok((sources, snapshot))
    }
}
fn valid_name(value: &str) -> bool {
    !value.is_empty() && value.len() <= 255 && !value.chars().any(char::is_control)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn explicit_vendor_inputs_are_read_once_and_secrets_never_enter_resolved_configuration() {
        let input:ConfigurationFileV1=serde_json::from_value(json!({"schema_version":1,"default_model":"qwen.work","sources":[{
            "alias":"work","display_name":"Work","protocol":"open_ai_responses","environment":{"schema_version":1,"api_key":"COMPANY_KEY","base_url":"COMPANY_URL","model":"COMPANY_MODEL"},
            "models":[{"alias":"qwen.work","display_name":"Work Qwen","quota":{"requests":20,"tokens":204800,"cost_microunits":20000000}},{"alias":"qwen.small","display_name":"Small Qwen","quota":{"requests":20,"tokens":204800,"cost_microunits":20000000},"model":"another-model"}]}]})).unwrap();
        let mut read = BTreeMap::<String, u8>::new();
        let (sources, values) = input
            .resolve(
                |name| {
                    *read.entry(name.to_owned()).or_default() += 1;
                    Ok(match name {
                        "COMPANY_KEY" => "secret-canary-value",
                        "COMPANY_URL" => "https://api.example.com/compatible-mode/v1",
                        "COMPANY_MODEL" => "example-model",
                        _ => panic!("ambient environment scanned"),
                    }
                    .to_owned())
                },
                |_| panic!("unexpected key file read"),
            )
            .unwrap();
        assert!(read.values().all(|count| *count == 1));
        assert_eq!(read.len(), 3);
        assert_eq!(sources[0].models.len(), 2);
        assert_eq!(sources[0].endpoint.base_path, "/compatible-mode");
        assert!(!serde_json::to_string(&sources)
            .unwrap()
            .contains("secret-canary-value"));
        assert_eq!(
            values.key(&sources[0].credential).unwrap().expose(),
            b"secret-canary-value"
        );
    }
    #[test]
    fn multiple_accounts_share_one_file_read_and_never_scan_unused_preset_variables() {
        let source = |alias: &str| json!({"alias":alias,"display_name":alias,"preset":"dashscope","api_key_file":"/run/keys/shared","base_url":"https://api.example.com/v1","models":[{"alias":format!("{alias}.chat"),"display_name":"Chat","quota":{"requests":20,"tokens":204800,"cost_microunits":20000000},"model":"qwen-example"}]});
        let input: ConfigurationFileV1 = serde_json::from_value(
            json!({"schema_version":1,"sources":[source("work"),source("personal")]}),
        )
        .unwrap();
        let mut reads = 0;
        let (sources, snapshot) = input
            .resolve(
                |_| panic!("unused preset variable read"),
                |path| {
                    assert_eq!(path, "/run/keys/shared");
                    reads += 1;
                    SensitiveModelApiKey::new(b"private-test-key".to_vec())
                        .map_err(|_| "invalid".to_owned())
                },
            )
            .unwrap();
        assert_eq!(reads, 1);
        assert_eq!(sources.len(), 2);
        for source in &sources {
            assert_eq!(
                snapshot.key(&source.credential).unwrap().expose(),
                b"private-test-key"
            );
        }
        assert!(!serde_json::to_string(&sources)
            .unwrap()
            .contains("private-test-key"));
    }
    #[test]
    fn cross_source_credential_metadata_overlap_fails_before_reading_inputs() {
        let source = |alias: &str, key: &str, model: &str| json!({"alias":alias,"display_name":alias,"protocol":"open_ai_responses","environment":{"schema_version":1,"api_key":key},"base_url":"https://api.example.com/v1","models":[{"alias":format!("{alias}.chat"),"display_name":"Chat","quota":{"requests":20,"tokens":204800,"cost_microunits":20000000},"model_environment_variable":model}]});
        let input:ConfigurationFileV1=serde_json::from_value(json!({"schema_version":1,"sources":[source("work","WORK_KEY","WORK_MODEL"),source("personal","PERSONAL_KEY","WORK_KEY")]})).unwrap();
        assert!(input
            .resolve(
                |_| panic!("must reject before reading"),
                |_| panic!("unexpected file")
            )
            .is_err());
    }
    #[test]
    fn quota_is_required_and_bounded_before_reading_any_secret() {
        let mut raw = json!({"schema_version":1,"sources":[{"alias":"work","display_name":"Work","preset":"dashscope","base_url":"https://example.com/v1","models":[{"alias":"work.chat","display_name":"Chat","model":"example"}]}]});
        assert!(serde_json::from_value::<ConfigurationFileV1>(raw.clone()).is_err());
        raw["sources"][0]["models"][0]["quota"] =
            json!({"requests":MAX_MODEL_QUOTA_VALUE+1,"tokens":0,"cost_microunits":0});
        let input: ConfigurationFileV1 = serde_json::from_value(raw).unwrap();
        assert!(input
            .resolve(
                |_| panic!("quota validation must precede secret reads"),
                |_| panic!("unexpected file read")
            )
            .is_err());
    }
}
