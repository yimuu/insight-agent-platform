//! Closed identifiers shared by configuration producers and the independent processes that
//! consume those configurations.
//!
//! Keeping these values in the credential-free contracts crate prevents a development
//! orchestrator from linking worker, RPC transport, or code-execution implementations merely to
//! construct an exact process configuration.

use crate::Sha256Digest;
use sha2::{Digest as _, Sha256};

pub const MODEL_WORKER_WORKLOAD_IDENTITY: &str = "spiffe://insight.platform/workload/model-worker";
pub const CAPABILITY_WORKER_WORKLOAD_IDENTITY: &str =
    "spiffe://insight.platform/workload/capability-worker";
pub const MCP_HOST_WORKLOAD_IDENTITY: &str = "spiffe://insight.platform/workload/mcp-host";
pub const MCP_DISCOVERY_WORKER_WORKLOAD_IDENTITY: &str =
    "spiffe://insight.platform/workload/mcp-discovery-worker";
pub const MCP_SUBSCRIPTION_WORKER_WORKLOAD_IDENTITY: &str =
    "spiffe://insight.platform/workload/mcp-subscription-worker";
pub const MCP_CLEANUP_WORKER_WORKLOAD_IDENTITY: &str =
    "spiffe://insight.platform/workload/mcp-cleanup-worker";
pub const MCP_CALLBACK_WORKLOAD_IDENTITY: &str =
    "spiffe://insight.platform/workload/mcp-callback-api";
pub const CONTEXT_WORKER_WORKLOAD_IDENTITY: &str =
    "spiffe://insight.platform/workload/context-worker";
pub const CONTEXT_DATASET_WORKER_WORKLOAD_IDENTITY: &str =
    "spiffe://insight.platform/workload/context-dataset-worker";
pub const EGRESS_BROKER_WORKLOAD_IDENTITY: &str =
    "spiffe://insight.platform/workload/egress-broker";

pub const BUILTIN_JSON_CODEC_ID: &str = "platform.json";
pub const BUILTIN_JSON_CODEC_VERSION: &str = "1.0.0";

pub fn builtin_json_codec_module_digest() -> Sha256Digest {
    builtin_capability_digest("builtin-json-codec-module")
}

pub fn builtin_json_http_protocol_contract_digest() -> Sha256Digest {
    builtin_capability_digest("builtin-json-http-protocol-contract")
}

pub fn builtin_json_http_request_mapping_digest() -> Sha256Digest {
    builtin_capability_digest("builtin-json-http-request-mapping")
}

pub fn builtin_json_http_response_mapping_digest() -> Sha256Digest {
    builtin_capability_digest("builtin-json-http-response-mapping")
}

pub fn builtin_json_http_error_mapping_digest() -> Sha256Digest {
    builtin_capability_digest("builtin-json-http-error-mapping")
}

pub fn builtin_json_grpc_protobuf_contract_digest() -> Sha256Digest {
    builtin_capability_digest("builtin-json-grpc-protobuf-contract")
}

pub fn builtin_json_grpc_request_mapping_digest() -> Sha256Digest {
    builtin_capability_digest("builtin-json-grpc-request-mapping")
}

pub fn builtin_json_grpc_response_mapping_digest() -> Sha256Digest {
    builtin_capability_digest("builtin-json-grpc-response-mapping")
}

pub fn builtin_json_grpc_error_mapping_digest() -> Sha256Digest {
    builtin_capability_digest("builtin-json-grpc-error-mapping")
}

pub fn builtin_json_mcp_output_mapping_digest() -> Sha256Digest {
    builtin_capability_digest("builtin-json-mcp-output-mapping")
}

fn builtin_capability_digest(domain: &str) -> Sha256Digest {
    let mut hasher = Sha256::new();
    hasher.update(b"insight.platform/v1/capability-worker/builtin\0");
    hasher.update(domain.as_bytes());
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(64);
    for byte in hasher.finalize() {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    format!("sha256:{encoded}")
        .parse()
        .expect("SHA-256 digest constructed from fixed bytes is valid")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_runtime_configuration_is_closed() {
        for identity in [
            MODEL_WORKER_WORKLOAD_IDENTITY,
            CAPABILITY_WORKER_WORKLOAD_IDENTITY,
            MCP_HOST_WORKLOAD_IDENTITY,
            MCP_DISCOVERY_WORKER_WORKLOAD_IDENTITY,
            MCP_SUBSCRIPTION_WORKER_WORKLOAD_IDENTITY,
            MCP_CLEANUP_WORKER_WORKLOAD_IDENTITY,
            MCP_CALLBACK_WORKLOAD_IDENTITY,
            CONTEXT_WORKER_WORKLOAD_IDENTITY,
            CONTEXT_DATASET_WORKER_WORKLOAD_IDENTITY,
            EGRESS_BROKER_WORKLOAD_IDENTITY,
        ] {
            assert!(identity.starts_with("spiffe://insight.platform/workload/"));
            assert_eq!(identity, identity.trim());
        }
        assert_eq!(builtin_json_codec_module_digest().as_str().len(), 71);
    }
}
