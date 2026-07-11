use std::sync::Arc;

use crate::events::hub::{EventError, EventSubscription};
use crate::events::protocol::RunEvent;

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
    live: EventSubscription,
    _lease: SubscriptionLease,
}

impl RunSubscription {
    pub(crate) fn new(
        run_id: impl Into<String>,
        live: EventSubscription,
        lease: SubscriptionLease,
    ) -> Self {
        Self {
            run_id: run_id.into(),
            live,
            _lease: lease,
        }
    }

    pub fn last_seq(&self) -> u64 {
        self.live.last_seq()
    }

    pub async fn recv(&mut self) -> Result<RunEvent, EventError> {
        self.live.recv().await
    }
}

pub struct AttachedRun {
    pub run_id: String,
    pub request_id: String,
    pub subscription: RunSubscription,
}
