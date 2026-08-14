use super::{
    decode_model_provider_sse, permanent, retryable_after_dispatch, ModelAdapterCancelOutcome,
    ModelAdapterCancelRequest, ModelAdapterFailure, ModelProviderByteStream,
    ModelProviderWireConnector, ModelProviderWireProtocol, ModelProviderWireRequest,
    ModelProviderWireStream,
};
use async_trait::async_trait;
use std::sync::Arc;

/// Sanitized HTTP response returned by the role-scoped Secret/Egress broker port.
///
/// The broker consumes the credential-free request, resolves exact Deployment and Secret
/// bindings, enforces DNS/network/TLS/redirect policy and strips all response headers and error
/// bodies except the bounded content type and status needed by this connector.
pub struct ModelProviderEgressResponse {
    pub status_code: u16,
    pub content_type: String,
    pub body: ModelProviderByteStream,
}

#[async_trait]
pub trait ModelProviderEgressBroker: Send + Sync {
    async fn open(
        &self,
        request: ModelProviderWireRequest,
    ) -> Result<ModelProviderEgressResponse, ModelAdapterFailure>;

    async fn cancel(
        &self,
        protocol: ModelProviderWireProtocol,
        request: ModelAdapterCancelRequest,
    ) -> Result<ModelAdapterCancelOutcome, ModelAdapterFailure>;
}

/// Production connector that keeps Provider credentials/endpoints behind an egress broker port.
pub struct BrokeredModelProviderWireConnector {
    broker: Arc<dyn ModelProviderEgressBroker>,
}

impl BrokeredModelProviderWireConnector {
    pub fn new(broker: Arc<dyn ModelProviderEgressBroker>) -> Self {
        Self { broker }
    }
}

#[async_trait]
impl ModelProviderWireConnector for BrokeredModelProviderWireConnector {
    async fn open(
        &self,
        request: ModelProviderWireRequest,
    ) -> Result<ModelProviderWireStream, ModelAdapterFailure> {
        let maximum_response_bytes = request.maximum_response_bytes;
        let deadline = request.deadline;
        let response = self.broker.open(request).await?;
        if !(100..=599).contains(&response.status_code) {
            return Err(permanent("model_egress_invalid_http_status"));
        }
        if response.status_code != 200 {
            return Err(
                if matches!(response.status_code, 408 | 429 | 500 | 502 | 503 | 504) {
                    retryable_after_dispatch("model_provider_http_retryable", deadline)
                } else {
                    permanent("model_provider_http_rejected")
                },
            );
        }
        if !valid_event_stream_content_type(&response.content_type) {
            return Err(permanent("model_provider_invalid_content_type"));
        }
        decode_model_provider_sse(response.body, maximum_response_bytes)
    }

    async fn cancel(
        &self,
        protocol: ModelProviderWireProtocol,
        request: ModelAdapterCancelRequest,
    ) -> Result<ModelAdapterCancelOutcome, ModelAdapterFailure> {
        self.broker.cancel(protocol, request).await
    }
}

fn valid_event_stream_content_type(value: &str) -> bool {
    if value.is_empty()
        || value.len() > 128
        || !value.is_ascii()
        || value.bytes().any(|byte| byte.is_ascii_control())
    {
        return false;
    }
    let mut parts = value.split(';');
    if !parts
        .next()
        .is_some_and(|media| media.trim().eq_ignore_ascii_case("text/event-stream"))
    {
        return false;
    }
    match (parts.next(), parts.next()) {
        (None, None) => true,
        (Some(parameter), None) => {
            let parameter = parameter.trim();
            parameter.split_once('=').is_some_and(|(name, value)| {
                name.trim().eq_ignore_ascii_case("charset")
                    && value.trim().eq_ignore_ascii_case("utf-8")
            })
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::valid_event_stream_content_type;

    #[test]
    fn content_type_is_closed_to_utf8_event_stream() {
        assert!(valid_event_stream_content_type("text/event-stream"));
        assert!(valid_event_stream_content_type(
            "Text/Event-Stream; charset=utf-8"
        ));
        assert!(!valid_event_stream_content_type("application/json"));
        assert!(!valid_event_stream_content_type(
            "text/event-stream; profile=unsafe"
        ));
        assert!(!valid_event_stream_content_type(
            "text/event-stream; charset=utf-8; charset=utf-8"
        ));
    }
}
