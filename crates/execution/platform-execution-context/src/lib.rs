//! Process-local execution correlation. It conveys no authorization or durable state.
use insight_platform_contracts::{SpanId, TraceFlags, TraceIdentityV1, W3cTraceParent};
use std::future::Future;
use tracing::Instrument as _;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TraceContextError {
    InvalidIdentity,
    MissingScope,
}
impl std::fmt::Display for TraceContextError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::InvalidIdentity => "execution trace identity is invalid",
            Self::MissingScope => "execution trace scope is missing",
        })
    }
}
impl std::error::Error for TraceContextError {}

tokio::task_local! {
    static ACTIVE_EXECUTION_TRACE: ExecutionTraceContext;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionTraceContext {
    pub identity: TraceIdentityV1,
    pub span_id: SpanId,
    pub flags: TraceFlags,
}

impl ExecutionTraceContext {
    pub fn start(identity: TraceIdentityV1, flags: TraceFlags) -> Result<Self, TraceContextError> {
        identity
            .validate()
            .map_err(|_| TraceContextError::InvalidIdentity)?;
        Ok(Self {
            identity,
            span_id: SpanId::new(),
            flags,
        })
    }

    pub fn receive(parent: W3cTraceParent) -> Self {
        Self {
            identity: TraceIdentityV1::new(parent.trace_id),
            span_id: SpanId::new(),
            flags: parent.flags,
        }
    }

    pub const fn outbound_parent(self) -> W3cTraceParent {
        W3cTraceParent::new(self.identity.trace_id, self.span_id, self.flags)
    }
}

pub async fn scope_trace<F>(context: ExecutionTraceContext, future: F) -> F::Output
where
    F: Future,
{
    let span = tracing::info_span!(
        "platform.internal_rpc",
        trace_id = %context.identity.trace_id,
        span_id = %context.span_id,
        trace_flags = ?context.flags,
    );
    ACTIVE_EXECUTION_TRACE
        .scope(context, future.instrument(span))
        .await
}

pub fn current_trace() -> Result<ExecutionTraceContext, TraceContextError> {
    ACTIVE_EXECUTION_TRACE
        .try_with(|context| *context)
        .map_err(|_| TraceContextError::MissingScope)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn concurrent_scopes_are_isolated_and_cannot_supply_a_missing_scope() {
        let first =
            ExecutionTraceContext::start(TraceIdentityV1::generate(), TraceFlags::Sampled).unwrap();
        let second =
            ExecutionTraceContext::start(TraceIdentityV1::generate(), TraceFlags::NotSampled)
                .unwrap();
        let read = || async {
            tokio::task::yield_now().await;
            current_trace().unwrap()
        };
        let (a, b) = tokio::join!(scope_trace(first, read()), scope_trace(second, read()));
        assert_eq!((a, b), (first, second));
        assert_eq!(current_trace(), Err(TraceContextError::MissingScope));
        scope_trace(first, async {
            assert_eq!(
                tokio::spawn(async { current_trace() }).await.unwrap(),
                Err(TraceContextError::MissingScope)
            );
        })
        .await;
    }
}
