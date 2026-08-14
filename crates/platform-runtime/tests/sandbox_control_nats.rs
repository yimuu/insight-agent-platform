use async_trait::async_trait;
use insight_platform_contracts::{
    checked_in_hard_limit_profile, ResourceId, ResourceKind, Sha256Digest,
};
use insight_platform_runtime::{
    NatsSandboxControlListener, NatsSandboxControlSignalSink, NatsSandboxControlTransportConfig,
};
use insight_platform_sandbox::{
    SandboxControlDelivery, SandboxControlError, SandboxControlSignalSink, SandboxStopReason,
    SandboxStopSignal,
};
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

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

fn signal(worker_process_generation_id: ResourceId) -> SandboxStopSignal {
    SandboxStopSignal {
        schema_version: 1,
        tenant_id: id(ResourceKind::Tenant, "3001"),
        sandbox_job_id: id(ResourceKind::SandboxJob, "3002"),
        invocation_id: id(ResourceKind::CapabilityInvocation, "3003"),
        job_id: id(ResourceKind::Job, "3004"),
        request_digest: digest('a'),
        attempt_no: 1,
        lease_generation: 1,
        worker_process_generation_id,
        reason: SandboxStopReason::Cancelled,
        source_event_id: id(ResourceKind::Event, "3005"),
        source_invocation_version: 2,
        source_event_payload_digest: digest('b'),
        signal_digest: digest('0'),
    }
    .seal()
    .unwrap()
}

#[derive(Default)]
struct RecordingLocalSink {
    first_signal_digest: std::sync::Mutex<Option<Sha256Digest>>,
}

#[async_trait]
impl SandboxControlSignalSink for RecordingLocalSink {
    async fn deliver(
        &self,
        signal: &SandboxStopSignal,
    ) -> Result<SandboxControlDelivery, SandboxControlError> {
        signal.validate()?;
        let mut first = self.first_signal_digest.lock().unwrap();
        if first.as_ref() == Some(&signal.signal_digest) {
            return Ok(SandboxControlDelivery::AlreadyStopped(signal.reason));
        }
        *first = Some(signal.signal_digest.clone());
        Ok(SandboxControlDelivery::Delivered)
    }
}

#[tokio::test]
async fn real_nats_routes_only_to_the_exact_executor_generation_when_configured() {
    let Ok(server) = std::env::var("PLATFORM_TEST_NATS_URL") else {
        return;
    };
    let controller_client = async_nats::connect(server.clone()).await.unwrap();
    let executor_client = async_nats::connect(server).await.unwrap();
    let config = NatsSandboxControlTransportConfig::from_profile(
        &checked_in_hard_limit_profile(),
        Duration::from_millis(500),
    )
    .unwrap();
    let worker_process_generation_id = id(ResourceKind::WorkerProcessGeneration, "3006");
    let local = Arc::new(RecordingLocalSink::default());
    let listener = NatsSandboxControlListener::bind(
        executor_client,
        config.clone(),
        worker_process_generation_id.clone(),
        local.clone(),
    )
    .await
    .unwrap();
    let shutdown = CancellationToken::new();
    let listener_shutdown = shutdown.clone();
    let listener_task = tokio::spawn(listener.run(listener_shutdown));

    let remote = NatsSandboxControlSignalSink::new(controller_client, config);
    let exact = signal(worker_process_generation_id);
    assert_eq!(
        remote.deliver(&exact).await.unwrap(),
        SandboxControlDelivery::Delivered
    );
    assert_eq!(
        remote.deliver(&exact).await.unwrap(),
        SandboxControlDelivery::AlreadyStopped(SandboxStopReason::Cancelled)
    );
    assert_eq!(
        local.first_signal_digest.lock().unwrap().as_ref(),
        Some(&exact.signal_digest)
    );

    let wrong_generation = signal(id(ResourceKind::WorkerProcessGeneration, "3007"));
    assert_eq!(
        remote.deliver(&wrong_generation).await,
        Err(SandboxControlError::TransportUnavailable)
    );

    shutdown.cancel();
    listener_task.await.unwrap().unwrap();
}
