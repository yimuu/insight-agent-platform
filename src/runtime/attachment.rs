use std::{collections::VecDeque, sync::Arc};

use crate::events::{
    hub::{EventError, EventSubscription},
    protocol::RunEvent,
};

pub(crate) trait LeaseOwner: Send + Sync {
    fn release_subscription(self: Arc<Self>, run_id: &str);
}

pub(crate) struct SubscriptionLease {
    owner: Arc<dyn LeaseOwner>,
    run_id: String,
    counted: bool,
}

impl SubscriptionLease {
    pub(crate) fn new(
        owner: Arc<dyn LeaseOwner>,
        run_id: impl Into<String>,
        counted: bool,
    ) -> Self {
        Self {
            owner,
            run_id: run_id.into(),
            counted,
        }
    }
}

impl Drop for SubscriptionLease {
    fn drop(&mut self) {
        if self.counted {
            Arc::clone(&self.owner).release_subscription(&self.run_id);
        }
    }
}

pub struct RunSubscription {
    pub run_id: String,
    replay: VecDeque<RunEvent>,
    live: Option<EventSubscription>,
    live_open: bool,
    replay_truncated: bool,
    last_seq: u64,
    _lease: Arc<SubscriptionLease>,
}

impl RunSubscription {
    pub(crate) fn new(
        run_id: impl Into<String>,
        replay: Vec<RunEvent>,
        live: Option<EventSubscription>,
        live_open: bool,
        replay_truncated: bool,
        after_seq: u64,
        lease: Arc<SubscriptionLease>,
    ) -> Self {
        Self {
            run_id: run_id.into(),
            replay: replay.into(),
            live,
            live_open,
            replay_truncated,
            last_seq: after_seq,
            _lease: lease,
        }
    }

    pub fn last_seq(&self) -> u64 {
        self.last_seq
    }

    pub async fn recv(&mut self) -> Result<RunEvent, EventError> {
        loop {
            let event = match self.replay.pop_front() {
                Some(event) => event,
                None if self.live_open => {
                    self.live
                        .as_mut()
                        .ok_or(EventError::SubscriptionClosed)?
                        .recv()
                        .await?
                }
                None if self.replay_truncated => {
                    self.replay_truncated = false;
                    return Err(EventError::ReplayTruncated {
                        last_seq: self.last_seq,
                    });
                }
                None => return Err(EventError::ReplayFinished),
            };
            if event.seq <= self.last_seq {
                continue;
            }
            self.last_seq = event.seq;
            return Ok(event);
        }
    }
}

pub struct AttachedRun {
    pub run_id: String,
    pub request_id: String,
    pub subscription: RunSubscription,
}
