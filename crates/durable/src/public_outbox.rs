//! Backend-neutral durable public-event outbox contracts.

use super::RepositoryErrorExt as _;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value;

use insight_engine::{PublicEventEnvelope, PublicEventId, PublicEventKind, RunId};

use super::RepositoryError;

/// PostgreSQL notification channel used only as a wake-up hint. Consumers
/// must always load the durable, already-redacted envelope by ID.
pub const PUBLIC_EVENT_NOTIFY_CHANNEL: &str = "insight_agent_public_events_v3";

/// Database-authoritative total-order cursor for one Run's public stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicEventPosition {
    run_id: RunId,
    causation_seq: u64,
    public_ordinal: u16,
    public_event_id: String,
}

impl PublicEventPosition {
    fn new(
        run_id: RunId,
        causation_seq: u64,
        public_ordinal: u16,
        public_event_id: String,
    ) -> Result<Self, RepositoryError> {
        if causation_seq == 0 || public_ordinal == 0 {
            return Err(RepositoryError::invalid_data());
        }
        PublicEventId::parse(public_event_id.clone())
            .map_err(|_| RepositoryError::invalid_data())?;
        Ok(Self {
            run_id,
            causation_seq,
            public_ordinal,
            public_event_id,
        })
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn causation_seq(&self) -> u64 {
        self.causation_seq
    }

    pub fn public_ordinal(&self) -> u16 {
        self.public_ordinal
    }

    pub fn public_event_id(&self) -> &str {
        &self.public_event_id
    }
}

/// Result of reading exactly the next permanent public-event receipt.
#[derive(Debug, Clone, PartialEq)]
pub enum OrderedPublicEventRead {
    UpToDate,
    Pending,
    RetentionGap,
    Event(Box<PublishedPublicEvent>),
}

/// One public event after the publication fence has committed.
///
/// The body is the already-redacted durable envelope. Notification payloads
/// never become event bodies and are useful only for locating this row.
#[derive(Debug, Clone, PartialEq)]
pub struct PublishedPublicEvent {
    run_id: RunId,
    public_event_id: String,
    terminal: bool,
    safe_envelope: PublicEventEnvelope,
    position: PublicEventPosition,
}

impl PublishedPublicEvent {
    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn public_event_id(&self) -> &str {
        &self.public_event_id
    }

    pub fn is_terminal(&self) -> bool {
        self.terminal
    }

    pub fn safe_envelope(&self) -> &PublicEventEnvelope {
        &self.safe_envelope
    }

    pub fn position(&self) -> &PublicEventPosition {
        &self.position
    }
}

/// Backend notification stream. A received ID is only a wake-up hint; callers
/// must use `load_published_public_event` before exposing anything publicly.
#[async_trait]
pub trait PublicEventNotificationStream: Send {
    async fn recv(&mut self) -> Result<String, RepositoryError>;
}

/// A fenced right to publish one durable public event.
///
/// The opaque token prevents an expired dispatcher from acknowledging a row
/// that has since been reclaimed, including when the new dispatcher happens
/// to reuse the same human-readable claimant name.
#[derive(Debug, Clone, PartialEq)]
pub struct PublicEventClaim {
    run_id: RunId,
    public_event_id: String,
    causation_event_id: String,
    event_kind: String,
    terminal: bool,
    claimant: String,
    claim_token: String,
    claim_expires_at: DateTime<Utc>,
    safe_envelope: PublicEventEnvelope,
}

impl PublicEventClaim {
    #[allow(clippy::too_many_arguments)]
    fn new(
        run_id: RunId,
        public_event_id: String,
        causation_event_id: String,
        event_kind: String,
        terminal: bool,
        claimant: String,
        claim_token: String,
        claim_expires_at: DateTime<Utc>,
        safe_envelope: Value,
    ) -> Result<Self, RepositoryError> {
        validate_label(&public_event_id)?;
        validate_label(&causation_event_id)?;
        validate_label(&event_kind)?;
        validate_label(&claimant)?;
        validate_label(&claim_token)?;
        let safe_envelope = decode_safe_envelope(
            &run_id,
            &public_event_id,
            &causation_event_id,
            &event_kind,
            terminal,
            safe_envelope,
        )?;
        Ok(Self {
            run_id,
            public_event_id,
            causation_event_id,
            event_kind,
            terminal,
            claimant,
            claim_token,
            claim_expires_at,
            safe_envelope,
        })
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn public_event_id(&self) -> &str {
        &self.public_event_id
    }

    pub fn causation_event_id(&self) -> &str {
        &self.causation_event_id
    }

    pub fn event_kind(&self) -> &str {
        &self.event_kind
    }

    pub fn is_terminal(&self) -> bool {
        self.terminal
    }

    pub fn claimant(&self) -> &str {
        &self.claimant
    }

    pub fn claim_token(&self) -> &str {
        &self.claim_token
    }

    pub fn claim_expires_at(&self) -> DateTime<Utc> {
        self.claim_expires_at
    }

    pub fn safe_envelope(&self) -> &PublicEventEnvelope {
        &self.safe_envelope
    }
}

/// Durable dispatcher contract. Claim and publication are separately fenced;
/// PostgreSQL publication atomically marks the row and emits `pg_notify`.
#[async_trait]
pub trait PublicEventOutboxRepository: Send + Sync {
    async fn claim_public_events(
        &self,
        claimant: &str,
        claim_seconds: u32,
        limit: u32,
    ) -> Result<Vec<PublicEventClaim>, RepositoryError>;

    async fn publish_public_event(
        &self,
        claim: &PublicEventClaim,
        nonterminal_retention_seconds: u32,
    ) -> Result<bool, RepositoryError>;

    /// Deletes only published, nonterminal events whose database-clock
    /// retention deadline has elapsed. Terminal events are never eligible.
    async fn prune_expired_public_events(&self, limit: u32) -> Result<u64, RepositoryError>;

    async fn load_terminal_public_event(
        &self,
        run_id: &RunId,
    ) -> Result<Option<PublicEventEnvelope>, RepositoryError>;

    /// Loads the committed terminal authority even before the separate
    /// publication fence completes. This is for stable Run reads only; event
    /// delivery must continue to use `load_terminal_public_event`.
    async fn load_terminal_public_event_authority(
        &self,
        run_id: &RunId,
    ) -> Result<Option<PublicEventEnvelope>, RepositoryError>;

    /// Loads one globally identified event only after publication committed.
    /// The unique `public_event_id` index is the authority behind this lookup.
    async fn load_published_public_event(
        &self,
        public_event_id: &str,
    ) -> Result<Option<PublishedPublicEvent>, RepositoryError>;

    async fn load_next_public_event(
        &self,
        run_id: &RunId,
        after: Option<&PublicEventPosition>,
    ) -> Result<OrderedPublicEventRead, RepositoryError>;

    /// Opens backend wake-up hints. SQLite deliberately has no cross-process
    /// stream and returns `None`.
    async fn open_public_event_notification_stream(
        &self,
    ) -> Result<Option<Box<dyn PublicEventNotificationStream>>, RepositoryError>;
}

fn validate_label(value: &str) -> Result<(), RepositoryError> {
    if value.is_empty()
        || value.len() > 256
        || value
            .chars()
            .any(|character| character.is_control() || character.is_whitespace())
    {
        return Err(RepositoryError::invalid_configuration());
    }
    Ok(())
}

fn decode_safe_envelope(
    run_id: &RunId,
    public_event_id: &str,
    causation_event_id: &str,
    event_kind: &str,
    terminal: bool,
    safe_envelope: Value,
) -> Result<PublicEventEnvelope, RepositoryError> {
    let envelope: PublicEventEnvelope =
        serde_json::from_value(safe_envelope).map_err(|_| RepositoryError::invalid_data())?;
    let envelope_terminal = matches!(
        envelope.kind(),
        PublicEventKind::RunCompleted
            | PublicEventKind::RunFailed
            | PublicEventKind::RunCancelled
            | PublicEventKind::RunInterrupted
    );
    if envelope.run_id() != run_id
        || envelope.public_event_id().as_str() != public_event_id
        || envelope.causation_event_id().as_str() != causation_event_id
        || envelope.kind().as_str() != event_kind
        || envelope_terminal != terminal
    {
        return Err(RepositoryError::invalid_data());
    }
    Ok(envelope)
}

/// Workspace-internal construction surface for storage adapters.
#[doc(hidden)]
pub mod adapter {
    use chrono::{DateTime, Utc};
    use serde_json::Value;

    use insight_engine::{PublicEventEnvelope, RunId};

    use super::{PublicEventClaim, PublicEventPosition, PublishedPublicEvent, RepositoryError};

    pub fn public_event_position(
        run_id: RunId,
        causation_seq: u64,
        public_ordinal: u16,
        public_event_id: String,
    ) -> Result<PublicEventPosition, RepositoryError> {
        PublicEventPosition::new(run_id, causation_seq, public_ordinal, public_event_id)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn public_event_claim(
        run_id: RunId,
        public_event_id: String,
        causation_event_id: String,
        event_kind: String,
        terminal: bool,
        claimant: String,
        claim_token: String,
        claim_expires_at: DateTime<Utc>,
        safe_envelope: Value,
    ) -> Result<PublicEventClaim, RepositoryError> {
        PublicEventClaim::new(
            run_id,
            public_event_id,
            causation_event_id,
            event_kind,
            terminal,
            claimant,
            claim_token,
            claim_expires_at,
            safe_envelope,
        )
    }

    pub fn published_public_event(
        run_id: RunId,
        public_event_id: String,
        terminal: bool,
        safe_envelope: PublicEventEnvelope,
        position: PublicEventPosition,
    ) -> PublishedPublicEvent {
        PublishedPublicEvent {
            run_id,
            public_event_id,
            terminal,
            safe_envelope,
            position,
        }
    }
}
