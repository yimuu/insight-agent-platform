//! Deployment-owned physical Context destinations; no business identity or authorization.
use crate::{
    CanonicalHttpEndpoint, CapabilityEndpointScheme, DataRegion, InstalledHttpCredentialInjection,
    Sha256Digest,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

pub const REMOTE_CONTEXT_EXECUTION_SCHEMA_VERSION: u32 = 2;
pub const MAX_REMOTE_CONTEXT_INSTALLATION_DESTINATIONS: usize = 16;
pub const MAX_REMOTE_CONTEXT_INSTALLATION_TRUST_BYTES: usize = 16_384;
pub const MAX_REMOTE_CONTEXT_INSTALLATION_REQUEST_BYTES: u32 = 65_536;
pub const MAX_REMOTE_CONTEXT_INSTALLATION_RESPONSE_BYTES: u32 = 1_048_576;

pub const REMOTE_CONTEXT_PROTOCOL_VERSION: u32 = 1;
/// The installed JSON protocol, independent of endpoints, tenant policies and builds.
pub fn remote_context_protocol_contract_digest() -> Sha256Digest {
    crate::canonical_digest(&serde_json::json!({
        "contract":"insight.context.remote_search.json", "version":1,
        "method":"POST", "media_type":"application/json",
        "request":"bounded_inline_query_projection_cursor_v1",
        "response":"closed_items_cursor_revision_v1"
    }))
    .expect("closed remote Context protocol")
    .parse()
    .expect("canonical digest")
}
pub fn remote_context_result_mapping_digest() -> Sha256Digest {
    crate::canonical_digest(&serde_json::json!({
        "contract":"insight.context.remote_search.result_mapping", "version":1,
        "source_identity":"canonical_json_sha256", "locator":"canonical_json_sha256",
        "content":"identity", "structured_fields":"identity", "score":"millionths",
        "classification":"bounded_by_current_request", "unknown_fields":"reject"
    }))
    .expect("closed remote Context mapping")
    .parse()
    .expect("canonical digest")
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstalledRemoteContextDestinationV1 {
    pub schema_version: u32,
    pub protocol_contract_digest: Sha256Digest,
    pub result_mapping_digest: Sha256Digest,
    pub endpoint: CanonicalHttpEndpoint,
    pub endpoint_identity_digest: Sha256Digest,
    pub region: DataRegion,
    pub credential_injections: Vec<InstalledHttpCredentialInjection>,
    pub trusted_root_pem: String,
    pub maximum_request_bytes: u32,
    pub maximum_response_bytes: u32,
}
impl InstalledRemoteContextDestinationV1 {
    /// TLS certificate parsing and actual DNS/public-address checks remain in Egress.
    pub fn validate_shape(&self) -> bool {
        self.schema_version == 1
            && self.protocol_contract_digest == remote_context_protocol_contract_digest()
            && self.result_mapping_digest == remote_context_result_mapping_digest()
            && self.endpoint.scheme == CapabilityEndpointScheme::Https
            && self.endpoint.validate().is_ok()
            && self.endpoint.canonical_digest().as_ref() == Ok(&self.endpoint_identity_digest)
            && !self.trusted_root_pem.is_empty()
            && self.trusted_root_pem.len() <= MAX_REMOTE_CONTEXT_INSTALLATION_TRUST_BYTES
            && self.credential_injections.len() <= crate::MAX_CONTEXT_CREDENTIALS
            && self
                .credential_injections
                .iter()
                .all(InstalledHttpCredentialInjection::validate_shape)
            && self
                .credential_injections
                .iter()
                .map(InstalledHttpCredentialInjection::purpose)
                .collect::<BTreeSet<_>>()
                .len()
                == self.credential_injections.len()
            && self
                .credential_injections
                .iter()
                .map(InstalledHttpCredentialInjection::header_name)
                .collect::<BTreeSet<_>>()
                .len()
                == self.credential_injections.len()
            && self.maximum_request_bytes > 0
            && self.maximum_request_bytes <= MAX_REMOTE_CONTEXT_INSTALLATION_REQUEST_BYTES
            && self.maximum_response_bytes > 0
            && self.maximum_response_bytes <= MAX_REMOTE_CONTEXT_INSTALLATION_RESPONSE_BYTES
    }
    pub fn same_selector(&self, other: &Self) -> bool {
        self.endpoint_identity_digest == other.endpoint_identity_digest
            && self.region == other.region
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn destination() -> InstalledRemoteContextDestinationV1 {
        let endpoint = CanonicalHttpEndpoint {
            scheme: CapabilityEndpointScheme::Https,
            host: "search.example.test".to_owned(),
            port: 443,
            base_path: "/query".to_owned(),
        };
        InstalledRemoteContextDestinationV1 {
            schema_version: 1,
            protocol_contract_digest: remote_context_protocol_contract_digest(),
            result_mapping_digest: remote_context_result_mapping_digest(),
            endpoint_identity_digest: endpoint.canonical_digest().unwrap(),
            endpoint,
            region: "cn-east-1".parse().unwrap(),
            credential_injections: vec![],
            // Only shape is checked here; Egress tests parse an actual certificate.
            trusted_root_pem: "public trust material".to_owned(),
            maximum_request_bytes: 65_536,
            maximum_response_bytes: 1_048_576,
        }
    }
    fn header(purpose: &str, name: &str) -> InstalledHttpCredentialInjection {
        InstalledHttpCredentialInjection::Header {
            purpose: purpose.parse().unwrap(),
            name: name.to_owned(),
        }
    }
    #[test]
    fn physical_context_grant_is_bounded_and_contains_no_business_identity() {
        let grant = destination();
        assert!(grant.validate_shape());
        let mut value = serde_json::to_value(&grant).unwrap();
        value["context_deployment"] = serde_json::json!({});
        assert!(serde_json::from_value::<InstalledRemoteContextDestinationV1>(value).is_err());
        let mut changed = grant.clone();
        changed.endpoint.host = "other.example.test".to_owned();
        assert!(!changed.validate_shape());
        for (request, response) in [(0, 1), (65_537, 1), (1, 0), (1, 1_048_577)] {
            let mut changed = grant.clone();
            changed.maximum_request_bytes = request;
            changed.maximum_response_bytes = response;
            assert!(!changed.validate_shape());
        }
        let mut changed = grant.clone();
        changed.trusted_root_pem = "x".repeat(16_384);
        assert!(changed.validate_shape());
        changed.trusted_root_pem.push('x');
        assert!(!changed.validate_shape());
        changed = grant.clone();
        changed.protocol_contract_digest = crate::canonical_digest(&serde_json::json!("other"))
            .unwrap()
            .parse()
            .unwrap();
        assert!(!changed.validate_shape());
    }
    #[test]
    fn installed_header_mapping_has_one_bounded_destination_per_purpose() {
        assert!(header("api_key", &"x".repeat(128)).validate_shape());
        assert!(!header("api_key", &"x".repeat(129)).validate_shape());
        for name in [
            "X-Key",
            "authorization",
            "host",
            "cookie",
            "connection",
            "content-length",
            "transfer-encoding",
            "x\r\nkey",
            "",
        ] {
            assert!(!header("api_key", name).validate_shape());
        }
        let mut grant = destination();
        grant.credential_injections = vec![
            InstalledHttpCredentialInjection::BearerAuthorization {
                purpose: "api_key".parse().unwrap(),
            },
            header("second_key", "x-key"),
        ];
        assert!(grant.validate_shape());
        grant
            .credential_injections
            .push(header("api_key", "x-other"));
        assert!(!grant.validate_shape());
        grant.credential_injections =
            vec![header("api_key", "x-key"), header("second_key", "x-key")];
        assert!(!grant.validate_shape());
        grant.credential_injections = vec![
            InstalledHttpCredentialInjection::BearerAuthorization {
                purpose: "api_key".parse().unwrap(),
            },
            InstalledHttpCredentialInjection::BearerAuthorization {
                purpose: "second_key".parse().unwrap(),
            },
        ];
        assert!(!grant.validate_shape());
        grant.credential_injections = vec![
            InstalledHttpCredentialInjection::BearerAuthorization {
                purpose: "api_key".parse().unwrap(),
            },
            header("second_key", "authorization"),
        ];
        assert!(!grant.validate_shape());
    }
}
