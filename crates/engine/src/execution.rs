use std::{
    error::Error,
    fmt,
    sync::{Arc, Mutex, MutexGuard, OnceLock},
    time::Duration,
};

use tokio::time::{sleep_until, Instant};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum StopReason {
    Cancelled = 1,
    Interrupted = 2,
    TimedOut = 3,
}

struct StopInner {
    token: CancellationToken,
    winner: Mutex<Option<StopReason>>,
    deadline: OnceLock<Instant>,
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
        winner: Mutex::new(None),
        deadline: OnceLock::new(),
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
        self.inner.claim_external(reason)
    }
}

impl StopSignal {
    fn bind_deadline(&self, deadline: Instant) -> Result<(), ()> {
        if let Some(existing) = self.inner.deadline.get() {
            return (*existing == deadline).then_some(()).ok_or(());
        }
        if self.inner.deadline.set(deadline).is_err()
            && self.inner.deadline.get().copied() != Some(deadline)
        {
            return Err(());
        }
        self.inner.claim_elapsed_deadline();
        Ok(())
    }

    pub fn reason(&self) -> Option<StopReason> {
        self.inner.claim_elapsed_deadline();
        self.inner.reason()
    }

    pub async fn stopped(&self) {
        if self.reason().is_some() {
            return;
        }
        if let Some(deadline) = self.inner.deadline.get().copied() {
            tokio::select! {
                _ = self.inner.token.cancelled() => {}
                _ = sleep_until(deadline) => {
                    self.inner.claim(StopReason::TimedOut);
                }
            }
        } else {
            self.inner.token.cancelled().await;
        }
    }
}

impl StopInner {
    fn claim_external(&self, requested: StopReason) -> bool {
        let requested_won = {
            let mut winner = self.lock_winner();
            if winner.is_some() {
                return false;
            }
            // Compare the absolute deadline and install the winner while
            // holding the same mutex. This is the linearization point: a
            // request before the boundary wins, while a request acquiring the
            // lock after the boundary installs TimedOut instead.
            let selected = if requested != StopReason::TimedOut && self.deadline_has_elapsed() {
                StopReason::TimedOut
            } else {
                requested
            };
            *winner = Some(selected);
            selected == requested
        };
        self.token.cancel();
        requested_won
    }

    fn claim(&self, reason: StopReason) -> bool {
        {
            let mut winner = self.lock_winner();
            if winner.is_some() {
                return false;
            }
            *winner = Some(reason);
        }
        self.token.cancel();
        true
    }

    fn reason(&self) -> Option<StopReason> {
        *self.lock_winner()
    }

    fn deadline_has_elapsed(&self) -> bool {
        self.deadline
            .get()
            .is_some_and(|deadline| Instant::now() >= *deadline)
    }

    fn claim_elapsed_deadline(&self) {
        if self.deadline_has_elapsed() {
            self.claim(StopReason::TimedOut);
        }
    }

    fn lock_winner(&self) -> MutexGuard<'_, Option<StopReason>> {
        self.winner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[derive(Clone)]
pub struct ExecutionControl {
    stop: StopSignal,
    deadline: Instant,
}

impl ExecutionControl {
    pub fn new(stop: StopSignal, timeout: Duration) -> Self {
        Self::with_deadline(stop, Instant::now() + timeout)
    }

    pub fn with_deadline(stop: StopSignal, deadline: Instant) -> Self {
        stop.bind_deadline(deadline)
            .expect("a stop signal has one immutable execution deadline");
        Self { stop, deadline }
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunErrorKind {
    Operation,
    Timeout,
    Stop,
    Infrastructure,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunError {
    code: &'static str,
    message: String,
    kind: RunErrorKind,
    stop_reason: Option<StopReason>,
}

impl RunError {
    pub fn operation(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            kind: RunErrorKind::Operation,
            stop_reason: None,
        }
    }

    pub fn infrastructure(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            kind: RunErrorKind::Infrastructure,
            stop_reason: None,
        }
    }

    pub fn code(&self) -> &'static str {
        self.code
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn kind(&self) -> RunErrorKind {
        self.kind
    }

    pub fn stop_reason(&self) -> Option<StopReason> {
        self.stop_reason
    }

    pub fn stopped(reason: StopReason) -> Self {
        let (code, message) = match reason {
            StopReason::Cancelled => ("RUN_CANCELLED", "run cancelled"),
            StopReason::Interrupted => ("RUN_INTERRUPTED", "run interrupted"),
            StopReason::TimedOut => ("RUN_TIMEOUT", "run timed out"),
        };
        Self {
            code,
            message: message.to_string(),
            kind: RunErrorKind::Stop,
            stop_reason: Some(reason),
        }
    }

    pub fn operation_timeout() -> Self {
        Self {
            code: "OPERATION_TIMEOUT",
            message: "operation execution timed out".to_string(),
            kind: RunErrorKind::Timeout,
            stop_reason: None,
        }
    }
}

impl fmt::Display for RunError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for RunError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn elapsed_deadline_wins_over_a_late_external_cancel() {
        let (controller, signal) = stop_pair();
        signal
            .bind_deadline(Instant::now() + Duration::from_secs(5))
            .unwrap();

        tokio::time::advance(Duration::from_secs(5)).await;

        assert!(!controller.request(StopReason::Cancelled));
        signal.stopped().await;
        assert_eq!(signal.reason(), Some(StopReason::TimedOut));
    }

    #[tokio::test(start_paused = true)]
    async fn external_cancel_winner_survives_a_later_deadline() {
        let (controller, signal) = stop_pair();
        signal
            .bind_deadline(Instant::now() + Duration::from_secs(5))
            .unwrap();

        assert!(controller.request(StopReason::Cancelled));
        tokio::time::advance(Duration::from_secs(5)).await;

        signal.stopped().await;
        assert_eq!(signal.reason(), Some(StopReason::Cancelled));
    }
}
