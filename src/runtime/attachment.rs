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
}

impl SubscriptionLease {
    pub(crate) fn new(owner: Arc<dyn LeaseOwner>, run_id: impl Into<String>) -> Self {
        Self {
            owner,
            run_id: run_id.into(),
        }
    }
}

impl Drop for SubscriptionLease {
    fn drop(&mut self) {
        Arc::clone(&self.owner).release_subscription(&self.run_id);
    }
}

pub struct RunSubscription {
    pub run_id: String,
    replay: VecDeque<RunEvent>,
    live: EventSubscription,
    last_seq: u64,
    _lease: Arc<SubscriptionLease>,
}

impl RunSubscription {
    pub(crate) fn new(
        run_id: impl Into<String>,
        replay: Vec<RunEvent>,
        live: EventSubscription,
        after_seq: u64,
        lease: Arc<SubscriptionLease>,
    ) -> Self {
        Self {
            run_id: run_id.into(),
            replay: replay.into(),
            live,
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
                None => self.live.recv().await?,
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
