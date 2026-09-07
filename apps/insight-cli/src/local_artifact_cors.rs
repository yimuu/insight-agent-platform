//! Development-only S3 transport policy. Signed upload authority remains owned by Artifact.
use crate::{strict_dependency_json, CliError};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LocalArtifactCorsConfiguration {
    #[serde(rename = "CORSRules")]
    rules: [LocalArtifactCorsRule; 1],
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase", deny_unknown_fields)]
struct LocalArtifactCorsRule {
    #[serde(rename = "ID")]
    id: String,
    allowed_origins: [String; 1],
    allowed_methods: [String; 1],
    allowed_headers: [String; 1],
    max_age_seconds: u32,
}
fn installed() -> LocalArtifactCorsConfiguration {
    LocalArtifactCorsConfiguration {
        rules: [LocalArtifactCorsRule {
            id: "insight-loopback-upload-v1".into(),
            allowed_origins: ["http://127.0.0.1:*".into()],
            allowed_methods: ["PUT".into()],
            allowed_headers: ["content-type".into()],
            max_age_seconds: 0,
        }],
    }
}
pub(crate) fn configuration_json() -> String {
    serde_json::to_string(&installed()).expect("closed local S3 CORS rule serializes")
}
pub(crate) fn validate_observed(response: &str) -> Result<(), CliError> {
    let value = strict_dependency_json(response, "S3 CORS")?;
    let configuration: LocalArtifactCorsConfiguration =
        serde_json::from_value(value).map_err(|_| {
            CliError::RuntimeUnavailable(
                "local Artifact bucket CORS does not match the closed development upload policy"
                    .into(),
            )
        })?;
    if configuration != installed() {
        return Err(CliError::RuntimeUnavailable(
            "local Artifact bucket CORS drifted from the development upload policy".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn development_cors_is_only_loopback_upload_and_rejects_drift_without_repair() {
        let valid = r#"{"CORSRules":[{"ID":"insight-loopback-upload-v1","AllowedOrigins":["http://127.0.0.1:*"],"AllowedMethods":["PUT"],"AllowedHeaders":["content-type"],"MaxAgeSeconds":0}]}"#;
        validate_observed(valid).unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(valid).unwrap(),
            serde_json::from_str::<serde_json::Value>(&configuration_json()).unwrap()
        );
        for (key, value) in [
            ("AllowedOrigins", json!(["*"])),
            ("AllowedOrigins", json!(["https://production.example"])),
            ("AllowedMethods", json!(["GET"])),
            ("AllowedHeaders", json!(["*"])),
            ("MaxAgeSeconds", json!(60)),
            ("ID", json!("unknown-version")),
            ("ExposeHeaders", json!(["*"])),
        ] {
            let mut changed: serde_json::Value = serde_json::from_str(valid).unwrap();
            changed["CORSRules"][0][key] = value;
            assert!(validate_observed(&changed.to_string()).is_err(), "{key}");
        }
        for invalid in [
            "{}",
            "{\"CORSRules\":[]}",
            "{\"CORSRules\":[],\"CORSRules\":[]}",
            "{ invalid",
        ] {
            assert!(validate_observed(invalid).is_err());
        }
        let mut extra: serde_json::Value = serde_json::from_str(valid).unwrap();
        let rule = extra["CORSRules"][0].clone();
        extra["CORSRules"].as_array_mut().unwrap().push(rule);
        assert!(validate_observed(&extra.to_string()).is_err());
    }
}
