//! Bounded Core NATS observation, with current PostgreSQL disclosure and lease checks.
#[derive(Debug)]
pub struct LiveTextInstallError;
impl std::fmt::Display for LiveTextInstallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("invalid live text configuration")
    }
}
impl std::error::Error for LiveTextInstallError {}
fn required_absolute_path(name: &str) -> Result<std::path::PathBuf, LiveTextInstallError> {
    let value = std::env::var(name).map_err(|_| LiveTextInstallError)?;
    let path = std::path::PathBuf::from(value);
    if !path.is_absolute() {
        return Err(LiveTextInstallError);
    }
    Ok(path)
}
use async_trait::async_trait;
use futures::{stream, StreamExt};
use insight_platform_api::{
    authentication::AuthenticatedPrincipal, run::RunApplicationError, run_live::*,
};
use insight_platform_contracts::ResourceId;
use insight_platform_models::{
    model_live_delta_subject, valid_model_live_namespace, ModelLiveTextDelta,
};
use insight_platform_postgres::repository::{
    run_live::LiveRunStatus, PgRepository, RepositoryError,
};
use serde::Deserialize;
use std::{
    collections::{BTreeMap, VecDeque},
    path::PathBuf,
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LiveTextConfig {
    pub servers: Vec<String>,
    pub namespace: String,
    pub connect_timeout_milliseconds: u64,
}
impl LiveTextConfig {
    pub fn validate(&self) -> Result<(), LiveTextInstallError> {
        if self.servers.is_empty()
            || self.servers.len() > 8
            || !valid_model_live_namespace(&self.namespace)
            || !(1..=10_000).contains(&self.connect_timeout_milliseconds)
            || self.servers.iter().any(|server| {
                reqwest::Url::parse(server).map_or(true, |u| {
                    u.scheme() != "tls"
                        || u.host_str().is_none()
                        || u.port().is_none()
                        || !u.username().is_empty()
                        || u.password().is_some()
                        || !matches!(u.path(), "" | "/")
                        || u.query().is_some()
                        || u.fragment().is_some()
                })
            })
        {
            return Err(LiveTextInstallError);
        }
        Ok(())
    }
}
#[derive(Clone)]
pub struct PgLiveText {
    repository: Arc<PgRepository>,
    config: LiveTextConfig,
    ca: PathBuf,
    certificate: PathBuf,
    key: PathBuf,
    connections: Arc<Mutex<BTreeMap<String, usize>>>,
}
struct Reservation {
    counts: Arc<Mutex<BTreeMap<String, usize>>>,
    principal: String,
}
impl Drop for Reservation {
    fn drop(&mut self) {
        if let Ok(mut map) = self.counts.lock() {
            if let Some(n) = map.get_mut(&self.principal) {
                *n -= 1;
                if *n == 0 {
                    map.remove(&self.principal);
                }
            }
        }
    }
}
impl PgLiveText {
    pub fn install(
        repository: Arc<PgRepository>,
        config: LiveTextConfig,
    ) -> Result<Self, LiveTextInstallError> {
        Self::from_tls_files(
            repository,
            config,
            required_absolute_path("PLATFORM_GATEWAY_NATS_CA_PATH")?,
            required_absolute_path("PLATFORM_GATEWAY_NATS_CERT_PATH")?,
            required_absolute_path("PLATFORM_GATEWAY_NATS_KEY_PATH")?,
        )
    }
    pub fn from_tls_files(
        repository: Arc<PgRepository>,
        config: LiveTextConfig,
        ca: PathBuf,
        certificate: PathBuf,
        key: PathBuf,
    ) -> Result<Self, LiveTextInstallError> {
        config.validate()?;
        if !ca.is_absolute() || !certificate.is_absolute() || !key.is_absolute() {
            return Err(LiveTextInstallError);
        }
        Ok(Self {
            repository,
            config,
            ca,
            certificate,
            key,
            connections: Arc::new(Mutex::new(BTreeMap::new())),
        })
    }
    fn reserve(
        &self,
        principal: &AuthenticatedPrincipal,
    ) -> Result<Reservation, RunApplicationError> {
        let key = format!("{}/{}", principal.tenant_id, principal.principal_id);
        let mut map = self
            .connections
            .lock()
            .map_err(|_| RunApplicationError::Unavailable)?;
        if map.values().sum::<usize>() >= LIVE_TEXT_MAX_CONNECTIONS
            || map.get(&key).copied().unwrap_or(0) >= LIVE_TEXT_MAX_PRINCIPAL_CONNECTIONS
        {
            return Err(RunApplicationError::Unavailable);
        }
        *map.entry(key.clone()).or_default() += 1;
        Ok(Reservation {
            counts: self.connections.clone(),
            principal: key,
        })
    }
    async fn authorize(
        &self,
        principal: &AuthenticatedPrincipal,
        run: &ResourceId,
        delta: Option<&ModelLiveTextDelta>,
    ) -> Result<(LiveRunStatus, bool), RunApplicationError> {
        if chrono::Utc::now() >= principal.credential_expires_at {
            return Err(RunApplicationError::Unauthenticated);
        }
        let authorized = tokio::time::timeout(
            Duration::from_secs(3),
            self.repository.authorize_live_run(
                &principal.tenant_id,
                &principal.principal_id,
                principal.principal_kind,
                principal.binding_generation,
                run,
                delta,
            ),
        )
        .await
        .map_err(|_| RunApplicationError::Unavailable)?
        .map_err(|error| match error {
            RepositoryError::PermissionDenied => RunApplicationError::Denied,
            RepositoryError::NotFound(_) => RunApplicationError::NotFound,
            RepositoryError::InvalidInput(_) => RunApplicationError::Invalid,
            _ => RunApplicationError::Unavailable,
        })?;
        if chrono::Utc::now() >= principal.credential_expires_at {
            return Err(RunApplicationError::Unauthenticated);
        }
        Ok(authorized)
    }
}
struct QueuedEnvelope {
    payload: bytes::Bytes,
    _bytes: tokio::sync::OwnedSemaphorePermit,
}
struct DrainTask(tokio::task::JoinHandle<()>);
impl Drop for DrainTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}
struct Observation {
    owner: PgLiveText,
    principal: AuthenticatedPrincipal,
    run: ResourceId,
    receiver: tokio::sync::mpsc::Receiver<QueuedEnvelope>,
    _drain: DrainTask,
    _reservation: Reservation,
    transport: Arc<AtomicU8>,
    end: tokio::time::Instant,
    attempts: BTreeMap<ResourceId, (u32, u64)>,
    pending: VecDeque<LiveTextBodyV1>,
    pending_delta: Option<ModelLiveTextDelta>,
    closed: bool,
}
fn close_reason(error: RunApplicationError) -> LiveTextCloseReason {
    match error {
        RunApplicationError::Unauthenticated => LiveTextCloseReason::Expired,
        RunApplicationError::Denied | RunApplicationError::NotFound => {
            LiveTextCloseReason::AuthorizationChanged
        }
        _ => LiveTextCloseReason::Unavailable,
    }
}
#[async_trait]
impl RunLiveApplication for PgLiveText {
    async fn open(
        &self,
        principal: AuthenticatedPrincipal,
        run: ResourceId,
    ) -> Result<LiveTextStream, RunApplicationError> {
        self.authorize(&principal, &run, None).await?;
        let reservation = self.reserve(&principal)?;
        let transport = Arc::new(AtomicU8::new(0));
        let callback = transport.clone();
        let connection = async_nats::ConnectOptions::new()
            .require_tls(true)
            .add_root_certificates(self.ca.clone())
            .add_client_certificate(self.certificate.clone(), self.key.clone())
            // Core NATS raw queue is independently bounded to eight <=1 MiB envelopes.
            .subscription_capacity(LIVE_TEXT_TRANSPORT_PENDING_MESSAGES)
            .event_callback(move |event| {
                let flag = callback.clone();
                async move {
                    match event {
                        async_nats::Event::SlowConsumer(_) => {
                            let _ =
                                flag.compare_exchange(0, 1, Ordering::Relaxed, Ordering::Relaxed);
                        }
                        async_nats::Event::Disconnected | async_nats::Event::Closed => {
                            let _ =
                                flag.compare_exchange(0, 2, Ordering::Relaxed, Ordering::Relaxed);
                        }
                        _ => {}
                    }
                }
            })
            .connection_timeout(Duration::from_millis(
                self.config.connect_timeout_milliseconds,
            ))
            .connect(self.config.servers.clone());
        let client = tokio::time::timeout(
            Duration::from_millis(self.config.connect_timeout_milliseconds),
            connection,
        )
        .await
        .map_err(|_| RunApplicationError::Unavailable)?
        .map_err(|_| RunApplicationError::Unavailable)?;
        if client.server_info().max_payload > LIVE_TEXT_MAX_PENDING_BYTES {
            return Err(RunApplicationError::Unavailable);
        }
        let subject = model_live_delta_subject(&self.config.namespace, &principal.tenant_id, &run)
            .map_err(|_| RunApplicationError::Invalid)?;
        let mut subscriber =
            tokio::time::timeout(Duration::from_secs(3), client.subscribe(subject))
                .await
                .map_err(|_| RunApplicationError::Unavailable)?
                .map_err(|_| RunApplicationError::Unavailable)?;
        tokio::time::timeout(Duration::from_secs(3), client.flush())
            .await
            .map_err(|_| RunApplicationError::Unavailable)?
            .map_err(|_| RunApplicationError::Unavailable)?;
        self.authorize(&principal, &run, None).await?;
        let end = tokio::time::Instant::now() + Duration::from_secs(LIVE_TEXT_MAX_SECONDS);
        let (sender, receiver) = tokio::sync::mpsc::channel(LIVE_TEXT_MAX_PENDING_MESSAGES);
        let budget = Arc::new(tokio::sync::Semaphore::new(LIVE_TEXT_MAX_PENDING_BYTES));
        let drain_transport = transport.clone();
        let drain = tokio::spawn(async move {
            let _client = client;
            loop {
                let message = tokio::select! {
                    _ = sender.closed() => break,
                    _ = tokio::time::sleep_until(end) => break,
                    message = subscriber.next() => message,
                };
                let Some(message) = message else {
                    break;
                };
                if message.payload.is_empty() || message.payload.len() > LIVE_TEXT_MAX_PENDING_BYTES
                {
                    drain_transport.store(2, Ordering::Relaxed);
                    break;
                }
                let Ok(permit) = budget
                    .clone()
                    .try_acquire_many_owned(message.payload.len() as u32)
                else {
                    drain_transport.store(1, Ordering::Relaxed);
                    break;
                };
                if sender
                    .try_send(QueuedEnvelope {
                        payload: message.payload,
                        _bytes: permit,
                    })
                    .is_err()
                {
                    drain_transport.store(1, Ordering::Relaxed);
                    break;
                }
            }
        });
        let state = Observation {
            owner: self.clone(),
            principal,
            run,
            receiver,
            _drain: DrainTask(drain),
            _reservation: reservation,
            transport,
            end,
            attempts: BTreeMap::new(),
            pending: VecDeque::from([LiveTextBodyV1::Opened { partial: true }]),
            pending_delta: None,
            closed: false,
        };
        Ok(stream::unfold(state,|mut s|async move {
            loop {
                if s.closed { return None; }
                let terminal = if tokio::time::Instant::now()>=s.end { Some(LiveTextCloseReason::DurationLimit) } else {
                    match s.owner.authorize(&s.principal,&s.run,None).await {
                        Ok((LiveRunStatus::Open,_))=>None,
                        Ok((LiveRunStatus::Cancelled,_))=>Some(LiveTextCloseReason::Cancelled),
                        Ok((LiveRunStatus::Terminal,_))=>Some(LiveTextCloseReason::Terminal),
                        Err(e)=>Some(close_reason(e)),
                    }
                };
                if let Some(reason)=terminal { s.pending.clear();s.pending.push_back(LiveTextBodyV1::Closed{reason}); }
                if let Some(body)=s.pending.pop_front() {
                    if matches!(body,LiveTextBodyV1::Text{..}) {
                        let Some(delta)=s.pending_delta.take() else {continue;};
                        match s.owner.authorize(&s.principal,&s.run,Some(&delta)).await {
                            Ok((LiveRunStatus::Open,true))=>{}, Ok(_)=>continue,
                            Err(e)=>{s.pending.push_back(LiveTextBodyV1::Closed{reason:close_reason(e)});continue;}
                        }
                    }
                    s.closed=matches!(body,LiveTextBodyV1::Closed{..});return Some((LiveTextFrameV1{schema_version:1,run_id:s.run.clone(),body},s));
                }
                let transport=s.transport.load(Ordering::Relaxed);
                if transport!=0 {
                    s.pending.push_back(LiveTextBodyV1::Closed{reason:LiveTextCloseReason::Unavailable});
                    let body=LiveTextBodyV1::Gap{reason:if transport==1{LiveTextGapReason::SlowConsumer}else{LiveTextGapReason::TransportReconnected}};
                    return Some((LiveTextFrameV1{schema_version:1,run_id:s.run.clone(),body},s));
                }
                let message=tokio::select! { msg=s.receiver.recv()=>msg, _=tokio::time::sleep(Duration::from_millis(LIVE_TEXT_AUTH_INTERVAL_MILLISECONDS))=>continue };
                let Some(message)=message else { s.pending.push_back(LiveTextBodyV1::Closed{reason:LiveTextCloseReason::Unavailable});continue; };
                if message.payload.len()>LIVE_TEXT_MAX_PENDING_BYTES { s.pending.push_back(LiveTextBodyV1::Closed{reason:LiveTextCloseReason::Unavailable});continue; }
                let Ok(delta)=serde_json::from_slice::<ModelLiveTextDelta>(&message.payload) else { s.pending.push_back(LiveTextBodyV1::Closed{reason:LiveTextCloseReason::Unavailable});continue; };
                match s.owner.authorize(&s.principal,&s.run,Some(&delta)).await {
                    Ok((LiveRunStatus::Open,true))=>{}, Ok(_)=>continue,
                    Err(e)=>{s.pending.push_back(LiveTextBodyV1::Closed{reason:close_reason(e)});continue;}
                }
                match project_text(&mut s.attempts, &delta) {
                    Ok(frames) => { s.pending_delta=Some(delta);s.pending.extend(frames); },
                    Err(()) => {s.pending.push_back(LiveTextBodyV1::Gap{reason:LiveTextGapReason::SequenceGap});s.pending.push_back(LiveTextBodyV1::Closed{reason:LiveTextCloseReason::Unavailable});}
                }
            }
        }).boxed())
    }
}

/// Projection state is bounded and belongs to this connection, not durable execution.
fn project_text(
    attempts: &mut BTreeMap<ResourceId, (u32, u64)>,
    delta: &ModelLiveTextDelta,
) -> Result<Vec<LiveTextBodyV1>, ()> {
    if delta.text.len() > LIVE_TEXT_MAX_FRAME_BYTES
        || (!attempts.contains_key(&delta.model_turn_id) && attempts.len() >= 128)
    {
        return Err(());
    }
    let previous = attempts.get(&delta.model_turn_id).copied();
    if previous.is_some_and(|(attempt, sequence)| {
        attempt > delta.attempt_no
            || (attempt == delta.attempt_no && sequence >= delta.text_sequence)
    }) {
        return Ok(vec![]);
    }
    let mut frames = Vec::with_capacity(3);
    if previous.is_none_or(|(attempt, _)| attempt != delta.attempt_no) {
        frames.push(LiveTextBodyV1::Reset {
            model_turn_id: delta.model_turn_id.clone(),
            attempt_no: delta.attempt_no,
        });
    }
    let expected = previous
        .filter(|(attempt, _)| *attempt == delta.attempt_no)
        .map_or(1, |(_, seq)| seq.saturating_add(1));
    if expected != delta.text_sequence {
        frames.push(LiveTextBodyV1::Gap {
            reason: if previous.is_none() {
                LiveTextGapReason::LateSubscription
            } else {
                LiveTextGapReason::SequenceGap
            },
        });
    }
    attempts.insert(
        delta.model_turn_id.clone(),
        (delta.attempt_no, delta.text_sequence),
    );
    frames.push(LiveTextBodyV1::Text {
        model_turn_id: delta.model_turn_id.clone(),
        attempt_no: delta.attempt_no,
        text_sequence: delta.text_sequence,
        text: delta.text.clone(),
    });
    let encoded = serde_json::to_vec(&LiveTextFrameV1 {
        schema_version: 1,
        run_id: delta.run_id.clone(),
        body: frames.last().expect("text frame").clone(),
    })
    .map_err(|_| ())?;
    if encoded.len() > LIVE_TEXT_MAX_FRAME_BYTES {
        return Err(());
    }
    Ok(frames)
}
#[cfg(test)]
mod tests {
    use super::*;
    use insight_platform_contracts::{DataClassification, ResourceKind};
    fn delta() -> ModelLiveTextDelta {
        let id = |kind: ResourceKind| {
            format!(
                "{}_0198f1cc-32e4-75e1-a9e8-d95ca0f80001",
                kind.descriptor().prefix
            )
            .parse()
            .unwrap()
        };
        ModelLiveTextDelta {
            schema_version: 2,
            tenant_id: id(ResourceKind::Tenant),
            run_id: id(ResourceKind::Run),
            model_turn_id: id(ResourceKind::ModelTurn),
            job_id: id(ResourceKind::Job),
            worker_process_generation_id: id(ResourceKind::WorkerProcessGeneration),
            attempt_no: 1,
            lease_generation: 1,
            transport_sequence: 1,
            text_sequence: 1,
            request_digest: format!("sha256:{}", "a".repeat(64)).parse().unwrap(),
            classification: DataClassification::Internal,
            text: "你好".into(),
        }
    }
    #[test]
    fn projection_ignores_private_transport_gaps_and_resets_attempts() {
        let mut attempts = BTreeMap::new();
        let mut text = delta();
        assert!(matches!(
            project_text(&mut attempts, &text).unwrap()[0],
            LiveTextBodyV1::Reset { .. }
        ));
        text.transport_sequence = 3;
        text.text_sequence = 2;
        assert!(matches!(
            project_text(&mut attempts, &text).unwrap().as_slice(),
            [LiveTextBodyV1::Text { .. }]
        ));
        text.attempt_no = 2;
        text.text_sequence = 1;
        text.transport_sequence = 1;
        assert!(matches!(
            project_text(&mut attempts, &text).unwrap()[0],
            LiveTextBodyV1::Reset { attempt_no: 2, .. }
        ));
        text.attempt_no = 1;
        assert!(project_text(&mut attempts, &text).unwrap().is_empty());
    }
    #[test]
    fn late_subscription_and_loss_are_explicit_and_oversize_is_not_truncated() {
        let mut attempts = BTreeMap::new();
        let mut text = delta();
        text.text_sequence = 3;
        text.transport_sequence = 5;
        assert!(matches!(
            project_text(&mut attempts, &text).unwrap()[1],
            LiveTextBodyV1::Gap {
                reason: LiveTextGapReason::LateSubscription
            }
        ));
        text.text_sequence = 5;
        text.transport_sequence = 7;
        assert!(matches!(
            project_text(&mut attempts, &text).unwrap()[0],
            LiveTextBodyV1::Gap {
                reason: LiveTextGapReason::SequenceGap
            }
        ));
        text.text = "x".repeat(LIVE_TEXT_MAX_FRAME_BYTES + 1);
        assert!(project_text(&mut attempts, &text).is_err());
    }
    #[test]
    fn config_requires_tls_and_closed_namespace() {
        let mut config = LiveTextConfig {
            servers: vec!["tls://localhost:4222".into()],
            namespace: "local".into(),
            connect_timeout_milliseconds: 3000,
        };
        assert!(config.validate().is_ok());
        config.namespace = "local.>".into();
        assert!(config.validate().is_err());
        config.namespace = "local".into();
        config.servers[0] = "nats://localhost:4222".into();
        assert!(config.validate().is_err());
    }
}
