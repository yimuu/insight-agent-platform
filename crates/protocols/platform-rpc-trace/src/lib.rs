//! Shared transport-only trace propagation for internal mTLS RPC.
//!
//! Durable trace identity remains owned by the caller's Run/Job/other execution snapshot. This
//! crate validates and projects that identity into one physical RPC hop; it never derives tenant,
//! principal, owner, fence, or idempotency authority from metadata.

use insight_platform_contracts::W3cTraceParent;
use insight_platform_execution_context::{current_trace, ExecutionTraceContext};
use tonic::{metadata::MetadataValue, Request, Status};

pub const TRACEPARENT_METADATA: &str = "traceparent";
pub const TRACESTATE_METADATA: &str = "tracestate";
pub const BAGGAGE_METADATA: &str = "baggage";

pub fn request_with_trace<T>(
    message: T,
    context: ExecutionTraceContext,
) -> Result<Request<T>, Status> {
    let mut request = Request::new(message);
    inject_trace(&mut request, context)?;
    Ok(request)
}

pub fn request_with_current_trace<T>(message: T) -> Result<Request<T>, Status> {
    request_with_trace(
        message,
        current_trace().map_err(|_| missing_trace_status())?,
    )
}

pub fn inject_trace<T>(
    request: &mut Request<T>,
    context: ExecutionTraceContext,
) -> Result<(), Status> {
    context
        .identity
        .validate()
        .map_err(|_| invalid_trace_status())?;
    if request.metadata().contains_key(TRACEPARENT_METADATA)
        || request.metadata().contains_key(TRACESTATE_METADATA)
        || request.metadata().contains_key(BAGGAGE_METADATA)
    {
        return Err(invalid_trace_status());
    }
    let wire = context.outbound_parent().to_string();
    let value = MetadataValue::try_from(wire).map_err(|_| invalid_trace_status())?;
    request.metadata_mut().insert(TRACEPARENT_METADATA, value);
    Ok(())
}

pub fn require_trace<T>(request: &mut Request<T>) -> Result<ExecutionTraceContext, Status> {
    if request.metadata().contains_key(TRACESTATE_METADATA)
        || request.metadata().contains_key(BAGGAGE_METADATA)
    {
        return Err(invalid_trace_status());
    }
    let parent = request
        .metadata()
        .get(TRACEPARENT_METADATA)
        .ok_or_else(missing_trace_status)?
        .to_str()
        .map_err(|_| invalid_trace_status())?
        .parse::<W3cTraceParent>()
        .map_err(|_| invalid_trace_status())?;
    let context = ExecutionTraceContext::receive(parent);
    request.extensions_mut().insert(context);
    Ok(context)
}

pub fn require_trace_interceptor(mut request: Request<()>) -> Result<Request<()>, Status> {
    require_trace(&mut request)?;
    Ok(request)
}

/// Tonic client interceptor that projects the current process-local hop into required metadata.
#[derive(Debug, Clone, Copy, Default)]
pub struct PropagateTrace;

impl tonic::service::Interceptor for PropagateTrace {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        inject_trace(
            &mut request,
            current_trace().map_err(|_| missing_trace_status())?,
        )?;
        Ok(request)
    }
}

pub fn trace_context<T>(request: &Request<T>) -> Result<ExecutionTraceContext, Status> {
    request
        .extensions()
        .get::<ExecutionTraceContext>()
        .copied()
        .ok_or_else(missing_trace_status)
}

fn missing_trace_status() -> Status {
    Status::invalid_argument("required internal traceparent metadata is missing")
}

fn invalid_trace_status() -> Status {
    Status::invalid_argument("internal trace context is invalid")
}

#[cfg(test)]
mod tests {
    use super::*;
    use insight_platform_contracts::{TraceFlags, TraceId, TraceIdentityV1};
    use insight_platform_execution_context::{scope_trace, TraceContextError};
    use std::{
        io,
        sync::{Arc, Mutex},
    };

    #[derive(Clone, Default)]
    struct CapturedTelemetry(Arc<Mutex<Vec<u8>>>);

    struct CapturedTelemetryWriter(Arc<Mutex<Vec<u8>>>);

    impl<'writer> tracing_subscriber::fmt::MakeWriter<'writer> for CapturedTelemetry {
        type Writer = CapturedTelemetryWriter;

        fn make_writer(&'writer self) -> Self::Writer {
            CapturedTelemetryWriter(Arc::clone(&self.0))
        }
    }

    impl io::Write for CapturedTelemetryWriter {
        fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buffer);
            Ok(buffer.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn identity() -> TraceIdentityV1 {
        TraceIdentityV1::new(
            "0af7651916cd43dd8448eb211c80319c"
                .parse::<TraceId>()
                .unwrap(),
        )
    }

    #[test]
    fn each_receiver_keeps_trace_and_creates_a_new_span() {
        let caller = ExecutionTraceContext::start(identity(), TraceFlags::Sampled).unwrap();
        let mut request = request_with_trace((), caller).unwrap();
        let receiver = require_trace(&mut request).unwrap();
        assert_eq!(receiver.identity, caller.identity);
        assert_eq!(receiver.flags, caller.flags);
        assert_ne!(receiver.span_id, caller.span_id);
        assert_eq!(trace_context(&request).unwrap(), receiver);
    }

    #[test]
    fn missing_malformed_or_extended_context_fails_before_decode() {
        let mut missing = Request::new(());
        assert_eq!(
            require_trace(&mut missing).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );

        let mut malformed = Request::new(());
        malformed.metadata_mut().insert(
            TRACEPARENT_METADATA,
            MetadataValue::from_static("00-invalid"),
        );
        assert_eq!(
            require_trace(&mut malformed).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );

        for forbidden in [TRACESTATE_METADATA, BAGGAGE_METADATA] {
            let caller = ExecutionTraceContext::start(identity(), TraceFlags::NotSampled).unwrap();
            let mut request = request_with_trace((), caller).unwrap();
            request
                .metadata_mut()
                .insert(forbidden, MetadataValue::from_static("forged=value"));
            assert_eq!(
                require_trace(&mut request).unwrap_err().code(),
                tonic::Code::InvalidArgument
            );
        }
    }

    #[test]
    fn callers_cannot_overwrite_existing_trace_metadata() {
        let caller = ExecutionTraceContext::start(identity(), TraceFlags::NotSampled).unwrap();
        let mut request = Request::new(());
        request.metadata_mut().insert(
            TRACEPARENT_METADATA,
            MetadataValue::from_static("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-00"),
        );
        assert_eq!(
            inject_trace(&mut request, caller).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
    }

    #[tokio::test]
    async fn client_interceptor_requires_scope_and_injects_exact_parent() {
        let mut interceptor = PropagateTrace;
        assert_eq!(
            tonic::service::Interceptor::call(&mut interceptor, Request::new(()))
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
        let caller = ExecutionTraceContext::start(identity(), TraceFlags::Sampled).unwrap();
        let request = scope_trace(caller, async {
            tonic::service::Interceptor::call(&mut interceptor, Request::new(()))
        })
        .await
        .unwrap();
        assert_eq!(
            request.metadata().get(TRACEPARENT_METADATA).unwrap(),
            caller.outbound_parent().to_string().as_str()
        );
    }

    #[tokio::test]
    async fn task_scope_supplies_client_calls_without_process_global_state() {
        let caller = ExecutionTraceContext::start(identity(), TraceFlags::NotSampled).unwrap();
        let request = scope_trace(caller, async { request_with_current_trace(()) })
            .await
            .unwrap();
        assert_eq!(
            request
                .metadata()
                .get(TRACEPARENT_METADATA)
                .unwrap()
                .to_str()
                .unwrap(),
            caller.outbound_parent().to_string()
        );
        assert_eq!(
            current_trace().unwrap_err(),
            TraceContextError::MissingScope
        );
    }

    #[tokio::test]
    async fn dynamic_trace_capture_contains_correlation_without_context_canaries() {
        const TRACESTATE_CANARY: &str = "vendor=trace-canary-49fa124a";
        const BAGGAGE_CANARY: &str = "private=baggage-canary-ff8ad715";
        const PAYLOAD_CANARY: &str = "payload-canary-4e27b98f";

        let captured = CapturedTelemetry::default();
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_span_events(tracing_subscriber::fmt::format::FmtSpan::NEW)
            .with_writer(captured.clone())
            .finish();
        tracing::subscriber::set_global_default(subscriber).unwrap();
        let context = ExecutionTraceContext::start(identity(), TraceFlags::Sampled).unwrap();
        scope_trace(context, async {
            tracing::info!(
                event_name = "rpc.canary_exercised",
                "RPC trace canary exercised"
            );
            let _opaque_payload = [TRACESTATE_CANARY, BAGGAGE_CANARY, PAYLOAD_CANARY];
            assert_eq!(current_trace().unwrap(), context);
        })
        .await;

        let telemetry = String::from_utf8(captured.0.lock().unwrap().clone()).unwrap();
        assert!(telemetry.contains("platform.internal_rpc"));
        assert!(telemetry.contains(&format!("trace_id={}", context.identity.trace_id)));
        assert!(telemetry.contains(&format!("span_id={}", context.span_id)));
        assert!(telemetry.contains("trace_flags=Sampled"));
        assert!(telemetry.contains("event_name=\"rpc.canary_exercised\""));
        for forbidden in [TRACESTATE_CANARY, BAGGAGE_CANARY, PAYLOAD_CANARY] {
            assert!(!telemetry.contains(forbidden), "trace leaked {forbidden}");
        }
    }
}
