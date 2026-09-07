//! Outbox transport composition. Provisioning and worker execution use separate binaries/credentials.
use async_nats::jetstream::{
    self,
    stream::{Config, DiscardPolicy, RetentionPolicy, StorageType},
};
use async_trait::async_trait;
use insight_platform_contracts::{
    CommittedEventNoticeV1, OutboxFailureCode, MAX_COMMITTED_EVENT_NOTICE_BYTES,
};
use insight_platform_deployment_contracts::outbox::{
    OutboxJetStreamContractV1, COMMITTED_EVENT_STREAM, COMMITTED_EVENT_SUBJECT,
};
use insight_platform_worker::outbox::CommittedEventPublisher;
use std::time::Duration;

/// Conversion from the single deployment owner. This does not install or update a stream.
pub fn stream_configuration(
    contract: &OutboxJetStreamContractV1,
) -> Result<Config, OutboxFailureCode> {
    contract
        .validate()
        .map_err(|_| OutboxFailureCode::ContractIncompatible)?;
    Ok(Config {
        name: COMMITTED_EVENT_STREAM.to_owned(),
        subjects: vec![COMMITTED_EVENT_SUBJECT.to_owned()],
        max_bytes: contract.maximum_bytes as i64,
        max_messages: contract.maximum_messages as i64,
        max_message_size: MAX_COMMITTED_EVENT_NOTICE_BYTES as i32,
        max_consumers: 32,
        storage: StorageType::File,
        retention: RetentionPolicy::Limits,
        discard: DiscardPolicy::New,
        num_replicas: usize::from(contract.replicas),
        max_age: Duration::ZERO,
        duplicate_window: Duration::from_secs(u64::from(contract.duplicate_window_seconds)),
        ..Default::default()
    })
}

pub struct JetStreamCommittedEventPublisher {
    context: jetstream::Context,
    timeout: Duration,
}
impl JetStreamCommittedEventPublisher {
    /// Startup verifies an already installed stream. No create/update request is ever sent here.
    pub async fn from_client(
        client: async_nats::Client,
        contract: &OutboxJetStreamContractV1,
        timeout: Duration,
    ) -> Result<Self, OutboxFailureCode> {
        if timeout.is_zero() || timeout > Duration::from_secs(30) {
            return Err(OutboxFailureCode::ContractIncompatible);
        }
        let expected = stream_configuration(contract)?;
        let context = jetstream::new(client);
        let stream = tokio::time::timeout(timeout, context.get_stream(COMMITTED_EVENT_STREAM))
            .await
            .map_err(|_| OutboxFailureCode::TransportUnavailable)?
            .map_err(|_| OutboxFailureCode::TransportUnavailable)?;
        let actual = &stream.cached_info().config;
        if actual.name != expected.name
            || actual.subjects != expected.subjects
            || actual.max_bytes != expected.max_bytes
            || actual.max_messages != expected.max_messages
            || actual.max_message_size != expected.max_message_size
            || actual.max_consumers != expected.max_consumers
            || actual.storage != expected.storage
            || actual.retention != expected.retention
            || actual.discard != expected.discard
            || actual.num_replicas != expected.num_replicas
            || actual.max_age != expected.max_age
            || actual.duplicate_window != expected.duplicate_window
            || actual.no_ack
            || actual.sealed
            || actual.allow_rollup
            || actual.republish.is_some()
            || actual.mirror.is_some()
            || actual.sources.is_some()
            || actual.subject_transform.is_some()
        {
            return Err(OutboxFailureCode::ContractIncompatible);
        }
        Ok(Self { context, timeout })
    }
}
#[async_trait]
impl CommittedEventPublisher for JetStreamCommittedEventPublisher {
    async fn publish(&self, notice: &CommittedEventNoticeV1) -> Result<(), OutboxFailureCode> {
        notice
            .validate()
            .map_err(|_| OutboxFailureCode::ContractIncompatible)?;
        let bytes =
            serde_json::to_vec(notice).map_err(|_| OutboxFailureCode::ContractIncompatible)?;
        if bytes.len() > MAX_COMMITTED_EVENT_NOTICE_BYTES {
            return Err(OutboxFailureCode::ContractIncompatible);
        }
        let publish = async {
            let mut headers = async_nats::HeaderMap::new();
            headers.insert("Nats-Msg-Id", notice.event_id.to_string());
            headers.insert("Nats-Expected-Stream", COMMITTED_EVENT_STREAM);
            let ack = self
                .context
                .publish_with_headers(COMMITTED_EVENT_SUBJECT, headers, bytes.into())
                .await
                .map_err(|_| OutboxFailureCode::TransportUnavailable)?
                .await
                .map_err(|_| OutboxFailureCode::TransportUnavailable)?;
            if ack.stream != COMMITTED_EVENT_STREAM || ack.sequence == 0 {
                return Err(OutboxFailureCode::ConfirmationInvalid);
            }
            Ok(())
        };
        tokio::time::timeout(self.timeout, publish)
            .await
            .map_err(|_| OutboxFailureCode::TransportUnavailable)?
    }
}
