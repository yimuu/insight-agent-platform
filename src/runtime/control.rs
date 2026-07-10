use std::{
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc,
    },
    time::Duration,
};

use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use super::RunError;

pub type ContentEmitter =
    Arc<dyn Fn(String) -> Pin<Box<dyn Future<Output = Result<(), RunError>> + Send>> + Send + Sync>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum StopReason {
    Cancelled = 1,
    Interrupted = 2,
}

struct StopInner {
    token: CancellationToken,
    reason: AtomicU8,
}

#[derive(Clone)]
pub struct StopController {
    inner: Arc<StopInner>,
}

#[derive(Clone)]
pub struct StopSignal {
    inner: Arc<StopInner>,
}

pub fn stop_pair() -> (StopController, StopSignal) {
    let inner = Arc::new(StopInner {
        token: CancellationToken::new(),
        reason: AtomicU8::new(0),
    });
    (
        StopController {
            inner: Arc::clone(&inner),
        },
        StopSignal { inner },
    )
}

impl StopController {
    pub fn request(&self, reason: StopReason) -> bool {
        if self
            .inner
            .reason
            .compare_exchange(0, reason as u8, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return false;
        }
        self.inner.token.cancel();
        true
    }
}

impl StopSignal {
    pub fn reason(&self) -> Option<StopReason> {
        match self.inner.reason.load(Ordering::Acquire) {
            1 => Some(StopReason::Cancelled),
            2 => Some(StopReason::Interrupted),
            _ => None,
        }
    }

    pub async fn stopped(&self) {
        if self.reason().is_none() {
            self.inner.token.cancelled().await;
        }
    }
}

#[derive(Clone)]
pub struct ExecutionControl {
    stop: StopSignal,
    deadline: Instant,
    emit_content: ContentEmitter,
    content_enabled: bool,
}

impl ExecutionControl {
    pub fn new<F, Fut>(stop: StopSignal, timeout: Duration, emit_content: F) -> Self
    where
        F: Fn(String) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), RunError>> + Send + 'static,
    {
        Self {
            stop,
            deadline: Instant::now() + timeout,
            emit_content: Arc::new(move |content| Box::pin(emit_content(content))),
            content_enabled: true,
        }
    }

    pub fn with_content_enabled(mut self, enabled: bool) -> Self {
        self.content_enabled = enabled;
        self
    }

    pub fn stop_reason(&self) -> Option<StopReason> {
        self.stop.reason()
    }

    pub async fn stopped(&self) {
        self.stop.stopped().await;
    }

    pub fn remaining(&self) -> Duration {
        self.deadline.saturating_duration_since(Instant::now())
    }

    pub async fn emit_content(&self, content: impl Into<String>) -> Result<(), RunError> {
        if !self.content_enabled {
            return Err(RunError::new(
                "CONTENT_EMIT_DISABLED",
                "content emission is disabled for this node",
            ));
        }
        if let Some(reason) = self.stop_reason() {
            return Err(RunError::stopped(reason));
        }
        (self.emit_content)(content.into()).await
    }
}
