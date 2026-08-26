//! Core NATS transport for exact Sandbox execution-control delivery.

use async_nats::{Client, Subscriber};
use async_trait::async_trait;
use futures::StreamExt;
use insight_platform_contracts::{
    HardLimitProfile, LimitUnit, ResourceId, ResourceKind, Sha256Digest,
};
use insight_platform_sandbox::{
    SandboxControlDelivery, SandboxControlError, SandboxControlSignalSink, SandboxStopSignal,
};
use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::Duration};
use tokio::time;
use tokio_util::sync::CancellationToken;

pub const NATS_SANDBOX_CONTROL_SUBJECT_PREFIX: &str = "insight.platform.v1.sandbox.control";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SandboxNatsDependencyOutcome {
    Success,
    Failure,
}

/// Receives only the outcome of an actual Core NATS operation. Subject, worker generation,
/// tenant/job identity, payload and error details never cross this port.
pub trait SandboxNatsDependencyObserver: Send + Sync {
    fn observe(&self, outcome: SandboxNatsDependencyOutcome);
}

#[derive(Debug)]
struct NoopSandboxNatsDependencyObserver;

impl SandboxNatsDependencyObserver for NoopSandboxNatsDependencyObserver {
    fn observe(&self, _outcome: SandboxNatsDependencyOutcome) {}
}

fn observe_nats(observer: &Arc<dyn SandboxNatsDependencyObserver>, success: bool) {
    observer.observe(if success {
        SandboxNatsDependencyOutcome::Success
    } else {
        SandboxNatsDependencyOutcome::Failure
    });
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NatsSandboxControlTransportConfig {
    maximum_payload_bytes: usize,
    request_timeout: Duration,
}

impl NatsSandboxControlTransportConfig {
    pub fn from_profile(
        profile: &HardLimitProfile,
        request_timeout: Duration,
    ) -> Result<Self, SandboxControlError> {
        profile
            .validate()
            .map_err(|_| SandboxControlError::InvalidCapacity)?;
        let limit = &profile.control_data.nats_payload_bytes;
        if limit.unit != LimitUnit::Bytes || request_timeout.is_zero() {
            return Err(SandboxControlError::InvalidCapacity);
        }
        let maximum_payload_bytes =
            usize::try_from(limit.q1_default).map_err(|_| SandboxControlError::InvalidCapacity)?;
        if maximum_payload_bytes == 0 {
            return Err(SandboxControlError::InvalidCapacity);
        }
        Ok(Self {
            maximum_payload_bytes,
            request_timeout,
        })
    }

    pub const fn maximum_payload_bytes(&self) -> usize {
        self.maximum_payload_bytes
    }

    pub const fn request_timeout(&self) -> Duration {
        self.request_timeout
    }

    fn subject_for(
        &self,
        worker_process_generation_id: &ResourceId,
    ) -> Result<String, SandboxControlError> {
        if worker_process_generation_id.kind() != ResourceKind::WorkerProcessGeneration {
            return Err(SandboxControlError::InvalidTransportEnvelope);
        }
        Ok(format!(
            "{NATS_SANDBOX_CONTROL_SUBJECT_PREFIX}.{worker_process_generation_id}"
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SandboxControlTransportResponse {
    schema_version: u32,
    signal_digest: Sha256Digest,
    delivery: SandboxControlDelivery,
}

#[derive(Clone)]
pub struct NatsSandboxControlSignalSink {
    client: Client,
    config: NatsSandboxControlTransportConfig,
    dependency_observer: Arc<dyn SandboxNatsDependencyObserver>,
}

impl NatsSandboxControlSignalSink {
    pub fn new(client: Client, config: NatsSandboxControlTransportConfig) -> Self {
        Self::new_with_observer(client, config, Arc::new(NoopSandboxNatsDependencyObserver))
    }

    pub fn new_with_observer(
        client: Client,
        config: NatsSandboxControlTransportConfig,
        dependency_observer: Arc<dyn SandboxNatsDependencyObserver>,
    ) -> Self {
        Self {
            client,
            config,
            dependency_observer,
        }
    }
}

#[async_trait]
impl SandboxControlSignalSink for NatsSandboxControlSignalSink {
    async fn deliver(
        &self,
        signal: &SandboxStopSignal,
    ) -> Result<SandboxControlDelivery, SandboxControlError> {
        signal.validate()?;
        let subject = self
            .config
            .subject_for(&signal.worker_process_generation_id)?;
        let request = encode_bounded(signal, self.config.maximum_payload_bytes)?;
        let response = time::timeout(
            self.config.request_timeout,
            self.client.request(subject, request.into()),
        )
        .await;
        observe_nats(&self.dependency_observer, matches!(&response, Ok(Ok(_))));
        let response = response
            .map_err(|_| SandboxControlError::TransportUnavailable)?
            .map_err(|_| SandboxControlError::TransportUnavailable)?;
        decode_response(
            &response.payload,
            self.config.maximum_payload_bytes,
            &signal.signal_digest,
        )
    }
}

pub struct NatsSandboxControlListener {
    client: Client,
    config: NatsSandboxControlTransportConfig,
    worker_process_generation_id: ResourceId,
    local_sink: Arc<dyn SandboxControlSignalSink>,
    subscriber: Subscriber,
    dependency_observer: Arc<dyn SandboxNatsDependencyObserver>,
}

impl NatsSandboxControlListener {
    pub async fn bind(
        client: Client,
        config: NatsSandboxControlTransportConfig,
        worker_process_generation_id: ResourceId,
        local_sink: Arc<dyn SandboxControlSignalSink>,
    ) -> Result<Self, SandboxControlError> {
        Self::bind_with_observer(
            client,
            config,
            worker_process_generation_id,
            local_sink,
            Arc::new(NoopSandboxNatsDependencyObserver),
        )
        .await
    }

    pub async fn bind_with_observer(
        client: Client,
        config: NatsSandboxControlTransportConfig,
        worker_process_generation_id: ResourceId,
        local_sink: Arc<dyn SandboxControlSignalSink>,
        dependency_observer: Arc<dyn SandboxNatsDependencyObserver>,
    ) -> Result<Self, SandboxControlError> {
        let subject = config.subject_for(&worker_process_generation_id)?;
        let subscriber = time::timeout(config.request_timeout, async {
            let subscriber = client
                .subscribe(subject)
                .await
                .map_err(|_| SandboxControlError::TransportUnavailable)?;
            client
                .flush()
                .await
                .map_err(|_| SandboxControlError::TransportUnavailable)?;
            Ok::<_, SandboxControlError>(subscriber)
        })
        .await;
        observe_nats(&dependency_observer, matches!(&subscriber, Ok(Ok(_))));
        let subscriber = subscriber.map_err(|_| SandboxControlError::TransportUnavailable)??;
        Ok(Self {
            client,
            config,
            worker_process_generation_id,
            local_sink,
            subscriber,
            dependency_observer,
        })
    }

    pub async fn run(mut self, shutdown: CancellationToken) -> Result<(), SandboxControlError> {
        loop {
            let message = tokio::select! {
                _ = shutdown.cancelled() => {
                    let result = self.subscriber.unsubscribe().await;
                    observe_nats(&self.dependency_observer, result.is_ok());
                    return Ok(());
                }
                message = self.subscriber.next() => message,
            };
            let Some(message) = message else {
                observe_nats(&self.dependency_observer, false);
                return Err(SandboxControlError::TransportUnavailable);
            };
            let Some(reply) = message.reply else {
                continue;
            };
            let Ok(signal) = decode_signal(
                &message.payload,
                self.config.maximum_payload_bytes,
                &self.worker_process_generation_id,
            ) else {
                continue;
            };
            let delivery = self.local_sink.deliver(&signal).await?;
            let response = SandboxControlTransportResponse {
                schema_version: 1,
                signal_digest: signal.signal_digest,
                delivery,
            };
            let response = encode_bounded(&response, self.config.maximum_payload_bytes)?;
            let result = self.client.publish(reply, response.into()).await;
            observe_nats(&self.dependency_observer, result.is_ok());
            result.map_err(|_| SandboxControlError::TransportUnavailable)?;
        }
    }
}

fn encode_bounded<T: Serialize>(
    value: &T,
    maximum_payload_bytes: usize,
) -> Result<Vec<u8>, SandboxControlError> {
    let encoded =
        serde_json::to_vec(value).map_err(|_| SandboxControlError::InvalidTransportEnvelope)?;
    if encoded.is_empty() || encoded.len() > maximum_payload_bytes {
        return Err(SandboxControlError::InvalidTransportEnvelope);
    }
    Ok(encoded)
}

fn decode_signal(
    payload: &[u8],
    maximum_payload_bytes: usize,
    worker_process_generation_id: &ResourceId,
) -> Result<SandboxStopSignal, SandboxControlError> {
    if payload.is_empty() || payload.len() > maximum_payload_bytes {
        return Err(SandboxControlError::InvalidTransportEnvelope);
    }
    let signal: SandboxStopSignal = serde_json::from_slice(payload)
        .map_err(|_| SandboxControlError::InvalidTransportEnvelope)?;
    signal.validate()?;
    if &signal.worker_process_generation_id != worker_process_generation_id {
        return Err(SandboxControlError::InvalidTransportEnvelope);
    }
    Ok(signal)
}

fn decode_response(
    payload: &[u8],
    maximum_payload_bytes: usize,
    expected_signal_digest: &Sha256Digest,
) -> Result<SandboxControlDelivery, SandboxControlError> {
    if payload.is_empty() || payload.len() > maximum_payload_bytes {
        return Err(SandboxControlError::InvalidTransportEnvelope);
    }
    let response: SandboxControlTransportResponse = serde_json::from_slice(payload)
        .map_err(|_| SandboxControlError::InvalidTransportEnvelope)?;
    if response.schema_version != 1 || &response.signal_digest != expected_signal_digest {
        return Err(SandboxControlError::InvalidTransportEnvelope);
    }
    Ok(response.delivery)
}

#[cfg(test)]
mod tests {
    use super::*;
    use insight_platform_contracts::{checked_in_hard_limit_profile, ResourceKind};

    fn id(kind: ResourceKind, suffix: &str) -> ResourceId {
        format!(
            "{}_0198f1c8-32e4-75e1-a9e8-d95ca0f4{suffix}",
            kind.descriptor().prefix
        )
        .parse()
        .unwrap()
    }

    fn digest(character: char) -> Sha256Digest {
        format!("sha256:{}", character.to_string().repeat(64))
            .parse()
            .unwrap()
    }

    fn signal() -> SandboxStopSignal {
        SandboxStopSignal {
            schema_version: 1,
            tenant_id: id(ResourceKind::Tenant, "1001"),
            sandbox_job_id: id(ResourceKind::Job, "1002"),
            invocation_id: id(ResourceKind::CapabilityInvocation, "1003"),
            job_id: id(ResourceKind::Job, "1002"),
            request_digest: digest('a'),
            attempt_no: 1,
            lease_generation: 2,
            worker_process_generation_id: id(ResourceKind::WorkerProcessGeneration, "1005"),
            reason: insight_platform_sandbox::SandboxStopReason::Cancelled,
            source_event_id: id(ResourceKind::Event, "1006"),
            source_invocation_version: 3,
            source_event_payload_digest: digest('b'),
            signal_digest: digest('0'),
        }
        .seal()
        .unwrap()
    }

    #[test]
    fn config_uses_reviewed_nats_payload_limit_and_exact_generation_subject() {
        let profile = checked_in_hard_limit_profile();
        let config =
            NatsSandboxControlTransportConfig::from_profile(&profile, Duration::from_millis(250))
                .unwrap();
        assert_eq!(
            config.maximum_payload_bytes(),
            usize::try_from(profile.control_data.nats_payload_bytes.q1_default).unwrap()
        );
        let worker = id(ResourceKind::WorkerProcessGeneration, "2001");
        assert_eq!(
            config.subject_for(&worker).unwrap(),
            format!("{NATS_SANDBOX_CONTROL_SUBJECT_PREFIX}.{worker}")
        );
    }

    #[test]
    fn response_and_signal_are_closed_bounded_and_exact() {
        let signal = signal();
        let response = SandboxControlTransportResponse {
            schema_version: 1,
            signal_digest: signal.signal_digest.clone(),
            delivery: SandboxControlDelivery::Delivered,
        };
        let encoded = encode_bounded(&response, 4_096).unwrap();
        assert_eq!(
            decode_response(&encoded, 4_096, &signal.signal_digest).unwrap(),
            SandboxControlDelivery::Delivered
        );
        assert_eq!(
            decode_response(&encoded, 4_096, &digest('f')),
            Err(SandboxControlError::InvalidTransportEnvelope)
        );

        let signal_bytes = encode_bounded(&signal, 4_096).unwrap();
        assert_eq!(
            decode_signal(&signal_bytes, 4_096, &signal.worker_process_generation_id).unwrap(),
            signal
        );
    }
}
