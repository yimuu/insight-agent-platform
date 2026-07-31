use std::fmt::Write as _;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::wire::{MetaMap, PromptArgument, MCP_LEGACY_PROTOCOL_VERSION, MCP_PROTOCOL_VERSION};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpTransportKind {
    StreamableHttp,
    Stdio,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "id")]
pub enum PrincipalScope {
    Service,
    Tenant(String),
    User(String),
}

/// Immutable identity of one discovered server authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpServerBindingIdentity {
    pub connection_id: String,
    pub server_id: String,
    pub protocol_version: String,
    pub transport: McpTransportKind,
    pub principal_scope: PrincipalScope,
    pub discovery_fingerprint: String,
}

impl McpServerBindingIdentity {
    pub fn validate(&self) -> Result<(), BindingIdentityError> {
        if !qualified_id(&self.connection_id)
            || self.server_id.is_empty()
            || self.server_id.len() > 512
            || !matches!(
                self.protocol_version.as_str(),
                MCP_PROTOCOL_VERSION | MCP_LEGACY_PROTOCOL_VERSION
            )
            || !lower_sha256(&self.discovery_fingerprint)
        {
            return Err(BindingIdentityError);
        }
        match &self.principal_scope {
            PrincipalScope::Service => {}
            PrincipalScope::Tenant(id) | PrincipalScope::User(id)
                if !id.is_empty() && id.len() <= 256 => {}
            PrincipalScope::Tenant(_) | PrincipalScope::User(_) => {
                return Err(BindingIdentityError);
            }
        }
        Ok(())
    }
}

/// Publication-frozen mapping between an MCP tool and platform identities.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpToolBinding {
    pub server: McpServerBindingIdentity,
    pub remote_name: String,
    pub action_id: String,
    pub model_tool_name: String,
    pub title: Option<String>,
    pub description: Option<String>,
    pub input_schema: Value,
    pub output_schema: Option<Value>,
    pub annotations: Option<MetaMap>,
    pub catalog_fingerprint: String,
    pub policy_fingerprint: String,
    pub descriptor_hash: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpResourceBindingKind {
    Resource,
    Template,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpResourceBinding {
    pub server: McpServerBindingIdentity,
    pub remote_uri: String,
    pub kind: McpResourceBindingKind,
    pub mime_allowlist: Vec<String>,
    pub max_content_bytes: usize,
    pub catalog_fingerprint: String,
    pub policy_fingerprint: String,
    pub descriptor_hash: String,
}

#[derive(Serialize)]
struct CanonicalResourceBinding<'a> {
    server: &'a McpServerBindingIdentity,
    remote_uri: &'a str,
    kind: McpResourceBindingKind,
    mime_allowlist: &'a [String],
    max_content_bytes: usize,
    catalog_fingerprint: &'a str,
    policy_fingerprint: &'a str,
}

impl McpResourceBinding {
    pub fn seal(
        server: McpServerBindingIdentity,
        remote_uri: String,
        kind: McpResourceBindingKind,
        mut mime_allowlist: Vec<String>,
        max_content_bytes: usize,
        catalog_fingerprint: String,
        policy_fingerprint: String,
    ) -> Result<Self, BindingIdentityError> {
        server.validate()?;
        mime_allowlist.sort();
        mime_allowlist.dedup();
        if remote_uri.is_empty()
            || remote_uri.len() > 8 * 1024
            || remote_uri.chars().any(char::is_control)
            || max_content_bytes == 0
            || max_content_bytes > 256 * 1024 * 1024
            || mime_allowlist.len() > 128
            || mime_allowlist.iter().any(|value| {
                value.is_empty() || value.len() > 256 || value.chars().any(char::is_control)
            })
            || !lower_sha256(&catalog_fingerprint)
            || !lower_sha256(&policy_fingerprint)
        {
            return Err(BindingIdentityError);
        }
        let canonical = serde_jcs::to_vec(&CanonicalResourceBinding {
            server: &server,
            remote_uri: &remote_uri,
            kind,
            mime_allowlist: &mime_allowlist,
            max_content_bytes,
            catalog_fingerprint: &catalog_fingerprint,
            policy_fingerprint: &policy_fingerprint,
        })
        .map_err(|_| BindingIdentityError)?;
        Ok(Self {
            server,
            remote_uri,
            kind,
            mime_allowlist,
            max_content_bytes,
            catalog_fingerprint,
            policy_fingerprint,
            descriptor_hash: hex_sha256(&canonical),
        })
    }

    pub fn verify(&self) -> Result<(), BindingIdentityError> {
        let expected = Self::seal(
            self.server.clone(),
            self.remote_uri.clone(),
            self.kind,
            self.mime_allowlist.clone(),
            self.max_content_bytes,
            self.catalog_fingerprint.clone(),
            self.policy_fingerprint.clone(),
        )?;
        if expected.descriptor_hash == self.descriptor_hash {
            Ok(())
        } else {
            Err(BindingIdentityError)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpPromptBinding {
    pub server: McpServerBindingIdentity,
    pub remote_name: String,
    pub title: Option<String>,
    pub description: Option<String>,
    pub arguments: Vec<PromptArgument>,
    pub catalog_fingerprint: String,
    pub policy_fingerprint: String,
    pub descriptor_hash: String,
}

#[derive(Serialize)]
struct CanonicalPromptBinding<'a> {
    server: &'a McpServerBindingIdentity,
    remote_name: &'a str,
    title: &'a Option<String>,
    description: &'a Option<String>,
    arguments: &'a [PromptArgument],
    catalog_fingerprint: &'a str,
    policy_fingerprint: &'a str,
}

impl McpPromptBinding {
    pub fn seal(
        server: McpServerBindingIdentity,
        remote_name: String,
        title: Option<String>,
        description: Option<String>,
        arguments: Vec<PromptArgument>,
        catalog_fingerprint: String,
        policy_fingerprint: String,
    ) -> Result<Self, BindingIdentityError> {
        server.validate()?;
        if remote_name.is_empty()
            || remote_name.len() > 128
            || title
                .as_ref()
                .is_some_and(|value| value.is_empty() || value.len() > 512)
            || description
                .as_ref()
                .is_some_and(|value| value.is_empty() || value.len() > 16 * 1024)
            || arguments.len() > 128
            || !lower_sha256(&catalog_fingerprint)
            || !lower_sha256(&policy_fingerprint)
        {
            return Err(BindingIdentityError);
        }
        let canonical = serde_jcs::to_vec(&CanonicalPromptBinding {
            server: &server,
            remote_name: &remote_name,
            title: &title,
            description: &description,
            arguments: &arguments,
            catalog_fingerprint: &catalog_fingerprint,
            policy_fingerprint: &policy_fingerprint,
        })
        .map_err(|_| BindingIdentityError)?;
        Ok(Self {
            server,
            remote_name,
            title,
            description,
            arguments,
            catalog_fingerprint,
            policy_fingerprint,
            descriptor_hash: hex_sha256(&canonical),
        })
    }

    pub fn verify(&self) -> Result<(), BindingIdentityError> {
        let expected = Self::seal(
            self.server.clone(),
            self.remote_name.clone(),
            self.title.clone(),
            self.description.clone(),
            self.arguments.clone(),
            self.catalog_fingerprint.clone(),
            self.policy_fingerprint.clone(),
        )?;
        if expected.descriptor_hash == self.descriptor_hash {
            Ok(())
        } else {
            Err(BindingIdentityError)
        }
    }
}

#[derive(Serialize)]
struct CanonicalToolBinding<'a> {
    server: &'a McpServerBindingIdentity,
    remote_name: &'a str,
    action_id: &'a str,
    model_tool_name: &'a str,
    title: &'a Option<String>,
    description: &'a Option<String>,
    input_schema: &'a Value,
    output_schema: &'a Option<Value>,
    annotations: &'a Option<MetaMap>,
    catalog_fingerprint: &'a str,
    policy_fingerprint: &'a str,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpToolBindingDescriptor {
    pub remote_name: String,
    pub action_id: String,
    pub model_tool_name: String,
    pub title: Option<String>,
    pub description: Option<String>,
    pub input_schema: Value,
    pub output_schema: Option<Value>,
    pub annotations: Option<MetaMap>,
}

impl McpToolBinding {
    pub fn seal(
        server: McpServerBindingIdentity,
        descriptor: McpToolBindingDescriptor,
        catalog_fingerprint: String,
        policy_fingerprint: String,
    ) -> Result<Self, BindingIdentityError> {
        let McpToolBindingDescriptor {
            remote_name,
            action_id,
            model_tool_name,
            title,
            description,
            input_schema,
            output_schema,
            annotations,
        } = descriptor;
        server.validate()?;
        if remote_name.is_empty()
            || remote_name.len() > 128
            || !qualified_id(&action_id)
            || !model_name(&model_tool_name)
            || title.as_ref().is_some_and(|value| value.len() > 512)
            || description
                .as_ref()
                .is_some_and(|value| value.len() > 16 * 1024)
            || !input_schema.is_object()
            || output_schema
                .as_ref()
                .is_some_and(|value| !value.is_object())
            || !lower_sha256(&catalog_fingerprint)
            || !lower_sha256(&policy_fingerprint)
        {
            return Err(BindingIdentityError);
        }
        let canonical = serde_jcs::to_vec(&CanonicalToolBinding {
            server: &server,
            remote_name: &remote_name,
            action_id: &action_id,
            model_tool_name: &model_tool_name,
            title: &title,
            description: &description,
            input_schema: &input_schema,
            output_schema: &output_schema,
            annotations: &annotations,
            catalog_fingerprint: &catalog_fingerprint,
            policy_fingerprint: &policy_fingerprint,
        })
        .map_err(|_| BindingIdentityError)?;
        let descriptor_hash = hex_sha256(&canonical);
        Ok(Self {
            server,
            remote_name,
            action_id,
            model_tool_name,
            title,
            description,
            input_schema,
            output_schema,
            annotations,
            catalog_fingerprint,
            policy_fingerprint,
            descriptor_hash,
        })
    }

    pub fn verify(&self) -> Result<(), BindingIdentityError> {
        let expected = Self::seal(
            self.server.clone(),
            McpToolBindingDescriptor {
                remote_name: self.remote_name.clone(),
                action_id: self.action_id.clone(),
                model_tool_name: self.model_tool_name.clone(),
                title: self.title.clone(),
                description: self.description.clone(),
                input_schema: self.input_schema.clone(),
                output_schema: self.output_schema.clone(),
                annotations: self.annotations.clone(),
            },
            self.catalog_fingerprint.clone(),
            self.policy_fingerprint.clone(),
        )?;
        if expected.descriptor_hash == self.descriptor_hash {
            Ok(())
        } else {
            Err(BindingIdentityError)
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BindingIdentityError;

impl std::fmt::Display for BindingIdentityError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("invalid MCP binding identity")
    }
}

impl std::error::Error for BindingIdentityError {}

fn hex_sha256(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        write!(&mut encoded, "{byte:02x}").expect("writing to String cannot fail");
    }
    encoded
}

fn lower_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn qualified_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value.is_ascii()
        && value.split('.').all(|segment| {
            let mut bytes = segment.bytes();
            bytes
                .next()
                .is_some_and(|byte| byte == b'_' || byte.is_ascii_alphabetic())
                && bytes.all(|byte| byte == b'_' || byte == b'-' || byte.is_ascii_alphanumeric())
        })
}

fn model_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn server() -> McpServerBindingIdentity {
        McpServerBindingIdentity {
            connection_id: "engineering.github".to_owned(),
            server_id: "https://example.test/mcp".to_owned(),
            protocol_version: MCP_PROTOCOL_VERSION.to_owned(),
            transport: McpTransportKind::StreamableHttp,
            principal_scope: PrincipalScope::Tenant("tenant-a".to_owned()),
            discovery_fingerprint: "a".repeat(64),
        }
    }

    #[test]
    fn binding_hash_is_stable_and_detects_mutation() {
        let mut binding = McpToolBinding::seal(
            server(),
            McpToolBindingDescriptor {
                remote_name: "repositories.search".to_owned(),
                action_id: "mcp.engineering.repositories-search".to_owned(),
                model_tool_name: "engineering_search".to_owned(),
                title: Some("Search repositories".to_owned()),
                description: Some("Searches the approved source catalog.".to_owned()),
                input_schema: json!({"type": "object"}),
                output_schema: Some(json!({"type": "object"})),
                annotations: None,
            },
            "b".repeat(64),
            "c".repeat(64),
        )
        .unwrap();
        let same = binding.clone();
        assert_eq!(binding.descriptor_hash, same.descriptor_hash);
        binding.remote_name.push_str(".changed");
        assert!(binding.verify().is_err());
    }

    #[test]
    fn binding_identity_accepts_only_the_two_explicit_protocol_eras() {
        let mut legacy = server();
        legacy.protocol_version = MCP_LEGACY_PROTOCOL_VERSION.to_owned();
        assert!(legacy.validate().is_ok());
        legacy.protocol_version = "2025-06-18".to_owned();
        assert!(legacy.validate().is_err());
    }

    #[test]
    fn resource_and_prompt_bindings_cover_catalog_and_local_policy() {
        let resource = McpResourceBinding::seal(
            server(),
            "repo://project/{path}".to_owned(),
            McpResourceBindingKind::Template,
            vec!["text/plain".to_owned()],
            1024,
            "b".repeat(64),
            "c".repeat(64),
        )
        .unwrap();
        assert!(resource.verify().is_ok());

        let mut prompt = McpPromptBinding::seal(
            server(),
            "review_change".to_owned(),
            Some("Review change".to_owned()),
            None,
            vec![PromptArgument {
                name: "change".to_owned(),
                title: None,
                description: None,
                required: Some(true),
            }],
            "d".repeat(64),
            "e".repeat(64),
        )
        .unwrap();
        prompt.arguments[0].required = Some(false);
        assert!(prompt.verify().is_err());
    }
}
