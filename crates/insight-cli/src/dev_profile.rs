use insight_platform_contracts::{canonical_digest, parse_strict_json, JsonLimits};
use serde_json::Value;
use std::collections::BTreeSet;

const MODEL: u8 = 1 << 0;
const REMOTE_CAPABILITY: u8 = 1 << 1;
const CONTEXT: u8 = 1 << 2;
const MCP: u8 = 1 << 3;
const SANDBOX: u8 = 1 << 4;
const ALL: u8 = MODEL | REMOTE_CAPABILITY | CONTEXT | MCP | SANDBOX;
const REGISTRY_BYTES: &[u8] = include_bytes!("../../../release/development-profile-v1.json");
const REGISTRY_SCHEMA_BYTES: &[u8] =
    include_bytes!("../../../release/development-profile-v1.schema.json");

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DevProfile {
    features: u8,
    offline: bool,
    from_source: bool,
}

pub fn registry_content_digest() -> Result<String, String> {
    canonical_digest(&registry()?).map_err(|error| error.to_string())
}

pub fn registry_schema_digest() -> String {
    digest_bytes(REGISTRY_SCHEMA_BYTES)
}

fn digest_bytes(bytes: &[u8]) -> String {
    use sha2::{Digest as _, Sha256};
    let digest = Sha256::digest(bytes);
    let mut encoded = String::with_capacity(71);
    encoded.push_str("sha256:");
    for byte in digest {
        encoded.push(char::from(b"0123456789abcdef"[usize::from(byte >> 4)]));
        encoded.push(char::from(b"0123456789abcdef"[usize::from(byte & 15)]));
    }
    encoded
}

impl DevProfile {
    pub const fn starter() -> Self {
        Self {
            features: 0,
            offline: false,
            from_source: false,
        }
    }

    pub const fn source_starter() -> Self {
        Self {
            from_source: true,
            ..Self::starter()
        }
    }

    pub fn parse(features: Option<&str>, offline: bool, from_source: bool) -> Result<Self, String> {
        if offline && from_source {
            return Err("--offline conflicts with --from-source".to_owned());
        }
        let mut bits = 0_u8;
        if let Some(features) = features {
            if features.is_empty() {
                return Err("--features requires a non-empty comma-separated set".to_owned());
            }
            let values = features.split(',').collect::<Vec<_>>();
            if values
                .iter()
                .any(|value| value.is_empty() || value.trim() != *value)
            {
                return Err(
                    "--features entries must be non-empty and contain no whitespace".to_owned(),
                );
            }
            if values.contains(&"all") {
                if values.len() != 1 {
                    return Err("feature all cannot be combined with another feature".to_owned());
                }
                bits = ALL;
            } else {
                let mut observed = BTreeSet::new();
                for value in values {
                    if !observed.insert(value) {
                        return Err(format!("duplicate development feature {value:?}"));
                    }
                    bits |= match value {
                        "model" => MODEL,
                        "remote-capability" => REMOTE_CAPABILITY,
                        "context" => CONTEXT,
                        "mcp" => MCP,
                        "sandbox" => SANDBOX,
                        _ => return Err(format!("unknown development feature {value:?}")),
                    };
                }
            }
        }
        Ok(Self {
            features: bits,
            offline,
            from_source,
        })
    }

    pub fn feature_names(self) -> Vec<&'static str> {
        [
            (CONTEXT, "context"),
            (MCP, "mcp"),
            (MODEL, "model"),
            (REMOTE_CAPABILITY, "remote-capability"),
            (SANDBOX, "sandbox"),
        ]
        .into_iter()
        .filter_map(|(bit, name)| (self.features & bit != 0).then_some(name))
        .collect()
    }

    pub fn label(self) -> String {
        let features = self.feature_names();
        if features.is_empty() {
            "starter".to_owned()
        } else if self.features == ALL {
            "all".to_owned()
        } else {
            format!("starter+{}", features.join(","))
        }
    }

    pub fn profile_digest(self, release_identity: &str) -> Result<String, String> {
        let registry_digest = canonical_digest(&registry()?).map_err(|error| error.to_string())?;
        canonical_digest(&serde_json::json!({
            "schema_version": 1,
            "registry_digest": registry_digest,
            "release_identity": release_identity,
            "features": self.feature_names(),
        }))
        .map_err(|error| error.to_string())
    }

    pub const fn offline(self) -> bool {
        self.offline
    }

    pub const fn is_from_source(self) -> bool {
        self.from_source
    }

    pub const fn has_features(self) -> bool {
        self.features != 0
    }

    pub const fn has_context(self) -> bool {
        self.features & CONTEXT != 0
    }

    pub const fn has_model(self) -> bool {
        self.features & MODEL != 0
    }

    pub const fn has_remote_capability(self) -> bool {
        self.features & REMOTE_CAPABILITY != 0
    }

    pub const fn has_mcp(self) -> bool {
        self.features & MCP != 0
    }

    pub const fn has_sandbox(self) -> bool {
        self.features & SANDBOX != 0
    }

    pub const fn needs_egress(self) -> bool {
        self.features & (MODEL | REMOTE_CAPABILITY | CONTEXT | MCP) != 0
    }

    pub fn is_additive_from(self, previous: &[String]) -> bool {
        let selected = self.feature_names();
        previous
            .iter()
            .all(|feature| selected.contains(&feature.as_str()))
    }

    pub fn includes_role(self, role: &str) -> bool {
        match role {
            "context-native" | "context-remote" | "context-subscription" | "context-dataset" => {
                self.features & CONTEXT != 0
            }
            "model-worker" => self.features & MODEL != 0,
            "capability-remote" => self.features & REMOTE_CAPABILITY != 0,
            "mcp-resource-host" => self.features & (MCP | CONTEXT) != 0,
            "mcp-host" | "mcp-discovery" | "mcp-subscription" | "mcp-cleanup" | "callback-api" => {
                self.features & MCP != 0
            }
            "security-authority" | "egress-broker" => {
                self.features & (MODEL | REMOTE_CAPABILITY | CONTEXT | MCP) != 0
            }
            _ => false,
        }
    }
}

fn registry() -> Result<Value, String> {
    let value = parse_strict_json(
        REGISTRY_BYTES,
        JsonLimits {
            max_bytes: 131_072,
            max_depth: 12,
            max_properties_per_object: 64,
            max_items_per_array: 64,
            max_string_bytes: 1_024,
        },
    )
    .map_err(|error| format!("embedded development feature registry is invalid: {error}"))?;
    let object = value
        .as_object()
        .ok_or_else(|| "embedded development feature registry is not an object".to_owned())?;
    let expected = BTreeSet::from([
        "aliases",
        "dependencies",
        "deployment",
        "environment_class",
        "features",
        "kind",
        "production_qualification",
        "qualification",
        "release_images",
        "schema_version",
        "starter",
    ]);
    if object.keys().map(String::as_str).collect::<BTreeSet<_>>() != expected
        || object.get("schema_version") != Some(&Value::from(1))
        || object.get("kind") != Some(&Value::from("insight.dev.feature-registry/v1"))
    {
        return Err("embedded development feature registry is not closed v1".to_owned());
    }
    let features = object
        .get("features")
        .and_then(Value::as_object)
        .ok_or_else(|| "embedded development feature registry has no features".to_owned())?;
    if features.keys().map(String::as_str).collect::<BTreeSet<_>>()
        != BTreeSet::from(["context", "mcp", "model", "remote-capability", "sandbox"])
    {
        return Err("embedded development feature set is not closed".to_owned());
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordering_and_all_have_one_canonical_digest() {
        let left = DevProfile::parse(Some("model,context"), false, false).unwrap();
        let right = DevProfile::parse(Some("context,model"), false, false).unwrap();
        assert_eq!(left.feature_names(), vec!["context", "model"]);
        assert_eq!(
            left.profile_digest("source:a").unwrap(),
            right.profile_digest("source:a").unwrap()
        );
        assert_eq!(
            DevProfile::parse(Some("all"), false, false)
                .unwrap()
                .feature_names()
                .len(),
            5
        );
    }

    #[test]
    fn unknown_duplicate_conflict_and_whitespace_fail_before_any_io() {
        for value in ["unknown", "model,model", "model, all", "all,model", ""] {
            assert!(
                DevProfile::parse(Some(value), false, false).is_err(),
                "{value:?}"
            );
        }
        assert!(DevProfile::parse(None, true, true).is_err());
    }

    #[test]
    fn role_closure_is_feature_scoped() {
        let starter = DevProfile::starter();
        assert!(!starter.includes_role("model-worker"));
        let context = DevProfile::parse(Some("context"), false, false).unwrap();
        assert!(context.includes_role("context-native"));
        assert!(context.includes_role("egress-broker"));
        assert!(context.includes_role("mcp-resource-host"));
        let model = DevProfile::parse(Some("model"), false, false).unwrap();
        assert!(model.includes_role("model-worker"));
        assert!(model.includes_role("security-authority"));
    }
}
