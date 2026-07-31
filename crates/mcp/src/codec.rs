use std::fmt;

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;

use crate::wire::{
    CoreMethod, JsonRpcErrorResponse, JsonRpcResponse, ModernRequest, RequestId, RequestMetadata,
    JSON_RPC_VERSION, MCP_PROTOCOL_VERSION,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProtocolLimits {
    pub max_message_bytes: usize,
    pub max_json_depth: usize,
    pub max_method_bytes: usize,
    pub max_error_data_bytes: usize,
}

impl Default for ProtocolLimits {
    fn default() -> Self {
        Self {
            max_message_bytes: 4 * 1024 * 1024,
            max_json_depth: 32,
            max_method_bytes: 128,
            max_error_data_bytes: 64 * 1024,
        }
    }
}

#[derive(Debug, Clone)]
pub struct McpCodec {
    limits: ProtocolLimits,
}

impl McpCodec {
    pub fn new(limits: ProtocolLimits) -> Result<Self, CodecError> {
        if limits.max_message_bytes == 0
            || limits.max_json_depth == 0
            || limits.max_method_bytes == 0
            || limits.max_error_data_bytes == 0
        {
            return Err(CodecError::Limits);
        }
        Ok(Self { limits })
    }

    pub fn modern() -> Self {
        Self {
            limits: ProtocolLimits::default(),
        }
    }

    pub fn encode<T: Serialize>(&self, message: &T) -> Result<Vec<u8>, CodecError> {
        let bytes = serde_json::to_vec(message).map_err(|_| CodecError::Malformed)?;
        self.check_bytes(&bytes)?;
        Ok(bytes)
    }

    pub fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, CodecError> {
        self.check_bytes(bytes)?;
        serde_json::from_slice(bytes).map_err(|_| CodecError::Malformed)
    }

    pub fn decode_request(&self, bytes: &[u8]) -> Result<ModernRequest, CodecError> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct RequestWire {
            jsonrpc: String,
            id: RequestId,
            method: String,
            params: Value,
        }

        let request: RequestWire = self.decode(bytes)?;
        if request.jsonrpc != JSON_RPC_VERSION
            || request.method.len() > self.limits.max_method_bytes
        {
            return Err(CodecError::InvalidEnvelope);
        }
        let method = CoreMethod::parse(&request.method).ok_or(CodecError::MethodNotFound)?;
        let object = request
            .params
            .as_object()
            .ok_or(CodecError::InvalidParams)?;
        let metadata = object
            .get("_meta")
            .cloned()
            .ok_or(CodecError::MissingRequestMetadata)
            .and_then(|value| {
                serde_json::from_value::<RequestMetadata>(value)
                    .map_err(|_| CodecError::InvalidRequestMetadata)
            })?;
        if metadata.protocol_version != MCP_PROTOCOL_VERSION {
            return Err(CodecError::UnsupportedProtocolVersion);
        }
        Ok(ModernRequest {
            id: request.id,
            method,
            metadata,
            params: request.params,
        })
    }

    pub fn decode_response<R: DeserializeOwned>(
        &self,
        bytes: &[u8],
        expected_id: &RequestId,
    ) -> Result<Result<R, JsonRpcErrorResponse>, CodecError> {
        self.check_bytes(bytes)?;
        let value: Value = serde_json::from_slice(bytes).map_err(|_| CodecError::Malformed)?;
        if value.get("error").is_some() {
            let error: JsonRpcErrorResponse =
                serde_json::from_value(value).map_err(|_| CodecError::InvalidEnvelope)?;
            if error.jsonrpc != JSON_RPC_VERSION
                || error.id.as_ref().is_some_and(|id| id != expected_id)
                || error.error.message.is_empty()
                || error.error.data.as_ref().is_some_and(|data| {
                    serde_json::to_vec(data)
                        .map(|bytes| bytes.len() > self.limits.max_error_data_bytes)
                        .unwrap_or(true)
                })
            {
                return Err(CodecError::InvalidEnvelope);
            }
            return Ok(Err(error));
        }
        let response: JsonRpcResponse<R> =
            serde_json::from_value(value).map_err(|_| CodecError::InvalidEnvelope)?;
        if response.jsonrpc != JSON_RPC_VERSION || &response.id != expected_id {
            return Err(CodecError::Correlation);
        }
        Ok(Ok(response.result))
    }

    fn check_bytes(&self, bytes: &[u8]) -> Result<(), CodecError> {
        if bytes.is_empty() || bytes.len() > self.limits.max_message_bytes {
            return Err(CodecError::MessageTooLarge);
        }
        let value: Value = serde_json::from_slice(bytes).map_err(|_| CodecError::Malformed)?;
        if json_depth(&value) > self.limits.max_json_depth {
            return Err(CodecError::JsonTooDeep);
        }
        Ok(())
    }
}

fn json_depth(value: &Value) -> usize {
    match value {
        Value::Array(values) => 1 + values.iter().map(json_depth).max().unwrap_or(0),
        Value::Object(values) => 1 + values.values().map(json_depth).max().unwrap_or(0),
        _ => 1,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodecError {
    Limits,
    MessageTooLarge,
    JsonTooDeep,
    Malformed,
    InvalidEnvelope,
    InvalidParams,
    MissingRequestMetadata,
    InvalidRequestMetadata,
    UnsupportedProtocolVersion,
    MethodNotFound,
    Correlation,
}

impl CodecError {
    pub const fn json_rpc_code(self) -> i64 {
        match self {
            Self::Malformed | Self::MessageTooLarge | Self::JsonTooDeep => -32700,
            Self::MethodNotFound => -32601,
            Self::InvalidParams | Self::MissingRequestMetadata | Self::InvalidRequestMetadata => {
                -32602
            }
            Self::UnsupportedProtocolVersion => -32022,
            Self::Limits | Self::InvalidEnvelope | Self::Correlation => -32600,
        }
    }
}

impl fmt::Display for CodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Limits => "invalid MCP codec limits",
            Self::MessageTooLarge => "MCP message exceeds the configured limit",
            Self::JsonTooDeep => "MCP JSON exceeds the configured depth limit",
            Self::Malformed => "malformed MCP JSON",
            Self::InvalidEnvelope => "invalid MCP JSON-RPC envelope",
            Self::InvalidParams => "invalid MCP request parameters",
            Self::MissingRequestMetadata => "MCP request metadata is required",
            Self::InvalidRequestMetadata => "invalid MCP request metadata",
            Self::UnsupportedProtocolVersion => "unsupported MCP protocol version",
            Self::MethodNotFound => "unknown MCP method",
            Self::Correlation => "MCP response correlation failed",
        })
    }
}

impl std::error::Error for CodecError {}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::wire::CoreMethod;

    fn meta() -> Value {
        json!({
            "io.modelcontextprotocol/protocolVersion": "2026-07-28",
            "io.modelcontextprotocol/clientCapabilities": {},
            "io.modelcontextprotocol/clientInfo": {
                "name": "insight-agent-platform",
                "version": "0.1.0"
            }
        })
    }

    #[test]
    fn decodes_every_modern_core_request_method() {
        let codec = McpCodec::modern();
        let methods = [
            CoreMethod::ServerDiscover,
            CoreMethod::ToolsList,
            CoreMethod::ToolsCall,
            CoreMethod::ResourcesList,
            CoreMethod::ResourceTemplatesList,
            CoreMethod::ResourcesRead,
            CoreMethod::PromptsList,
            CoreMethod::PromptsGet,
            CoreMethod::CompletionComplete,
            CoreMethod::SubscriptionsListen,
        ];
        for (index, method) in methods.into_iter().enumerate() {
            let message = json!({
                "jsonrpc": "2.0",
                "id": index as i64,
                "method": method.as_str(),
                "params": {"_meta": meta()}
            });
            let decoded = codec
                .decode_request(&serde_json::to_vec(&message).unwrap())
                .unwrap();
            assert_eq!(decoded.method, method);
        }
    }

    #[test]
    fn rejects_unknown_method_version_and_correlation() {
        let codec = McpCodec::modern();
        let unknown = json!({
            "jsonrpc":"2.0","id":1,"method":"roots/list","params":{"_meta":meta()}
        });
        assert_eq!(
            codec.decode_request(&serde_json::to_vec(&unknown).unwrap()),
            Err(CodecError::MethodNotFound)
        );

        let mut wrong_version = meta();
        wrong_version["io.modelcontextprotocol/protocolVersion"] = json!("2025-11-25");
        let request = json!({
            "jsonrpc":"2.0","id":1,"method":"tools/list","params":{"_meta":wrong_version}
        });
        assert_eq!(
            codec.decode_request(&serde_json::to_vec(&request).unwrap()),
            Err(CodecError::UnsupportedProtocolVersion)
        );

        let response = json!({"jsonrpc":"2.0","id":2,"result":{}});
        assert_eq!(
            codec.decode_response::<Value>(
                &serde_json::to_vec(&response).unwrap(),
                &RequestId::Integer(1)
            ),
            Err(CodecError::Correlation)
        );
    }

    #[test]
    fn rejects_oversize_and_deep_messages_without_echoing_bodies() {
        let codec = McpCodec::new(ProtocolLimits {
            max_message_bytes: 64,
            max_json_depth: 3,
            max_method_bytes: 32,
            max_error_data_bytes: 16,
        })
        .unwrap();
        assert_eq!(
            codec.decode::<Value>(
                br#"{"value":"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"}"#
            ),
            Err(CodecError::MessageTooLarge)
        );
        assert_eq!(
            codec.decode::<Value>(br#"{"a":{"b":{"c":1}}}"#),
            Err(CodecError::JsonTooDeep)
        );
        assert!(!CodecError::Malformed.to_string().contains("value"));
    }
}
