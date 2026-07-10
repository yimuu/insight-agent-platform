use std::{
    future::Future,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use tokio::{
    sync::{mpsc, oneshot, watch},
    time::timeout,
};

use crate::{
    events::protocol::RunEvent,
    history::{
        repository::{HistoryError, RunRepository},
        types::{NodeOutputRecord, TerminalUpdate},
    },
};

use super::hub::EventError;

enum JournalCommand {
    Append {
        event: RunEvent,
        reply: oneshot::Sender<Result<(), EventError>>,
    },
    PutOutput {
        output: NodeOutputRecord,
        reply: oneshot::Sender<Result<(), EventError>>,
    },
    Finish {
        update: TerminalUpdate,
        event: Box<RunEvent>,
        reply: oneshot::Sender<Result<bool, EventError>>,
    },
    Flush(oneshot::Sender<Result<(), EventError>>),
}

struct EventJournalInner {
    sender: mpsc::Sender<JournalCommand>,
    stop: watch::Sender<bool>,
    stopped: watch::Receiver<bool>,
    healthy: Arc<AtomicBool>,
    operation_timeout: Duration,
}

#[derive(Clone)]
pub(crate) struct EventJournal {
    inner: Arc<EventJournalInner>,
}

impl EventJournal {
    pub(crate) fn new(
        repository: Arc<dyn RunRepository>,
        capacity: usize,
        batch_size: usize,
        operation_timeout: Duration,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(capacity.max(1));
        let (stop, stop_receiver) = watch::channel(false);
        let (stopped_sender, stopped) = watch::channel(false);
        let healthy = Arc::new(AtomicBool::new(true));
        let worker_health = Arc::clone(&healthy);
        tokio::spawn(async move {
            run_worker(
                receiver,
                repository,
                batch_size.max(1),
                operation_timeout,
                stop_receiver,
            )
            .await;
            worker_health.store(false, Ordering::Release);
            let _ = stopped_sender.send(true);
        });
        Self {
            inner: Arc::new(EventJournalInner {
                sender,
                stop,
                stopped,
                healthy,
                operation_timeout,
            }),
        }
    }

    pub(crate) async fn append(&self, event: RunEvent) -> Result<(), EventError> {
        let (reply, response) = oneshot::channel();
        self.try_send(JournalCommand::Append { event, reply })?;
        wait_for_response(response).await?
    }

    pub(crate) async fn put_output(&self, output: NodeOutputRecord) -> Result<(), EventError> {
        let (reply, response) = oneshot::channel();
        self.try_send(JournalCommand::PutOutput { output, reply })?;
        wait_for_response(response).await?
    }

    pub(crate) async fn finish(
        &self,
        update: TerminalUpdate,
        event: RunEvent,
    ) -> Result<bool, EventError> {
        let (reply, response) = oneshot::channel();
        self.try_send(JournalCommand::Finish {
            update,
            event: Box::new(event),
            reply,
        })?;
        wait_for_response(response).await?
    }

    pub(crate) async fn flush(&self) -> Result<(), EventError> {
        let (reply, response) = oneshot::channel();
        self.try_send(JournalCommand::Flush(reply))?;
        wait_for_response(response).await?
    }

    pub(crate) async fn close_and_wait(&self) -> Result<(), EventError> {
        let mut stopped = self.inner.stopped.clone();
        if *stopped.borrow() {
            return Ok(());
        }
        self.inner.healthy.store(false, Ordering::Release);
        let _ = self.inner.stop.send(true);
        timeout(self.inner.operation_timeout, async {
            while !*stopped.borrow() {
                stopped
                    .changed()
                    .await
                    .map_err(|_| EventError::JournalClosed)?;
            }
            Ok::<(), EventError>(())
        })
        .await
        .map_err(|_| EventError::JournalOperationTimeout)??;
        Ok(())
    }

    pub(crate) fn is_healthy(&self) -> bool {
        self.inner.healthy.load(Ordering::Acquire)
    }

    fn try_send(&self, command: JournalCommand) -> Result<(), EventError> {
        self.inner
            .sender
            .try_send(command)
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => EventError::JournalCapacityExceeded,
                mpsc::error::TrySendError::Closed(_) => EventError::JournalClosed,
            })
    }
}

async fn wait_for_response<T>(
    response: oneshot::Receiver<Result<T, EventError>>,
) -> Result<Result<T, EventError>, EventError> {
    response.await.map_err(|_| EventError::JournalClosed)
}

async fn run_worker(
    mut receiver: mpsc::Receiver<JournalCommand>,
    repository: Arc<dyn RunRepository>,
    batch_size: usize,
    operation_timeout: Duration,
    mut stop: watch::Receiver<bool>,
) {
    let mut pending = None;
    loop {
        let command = match pending.take() {
            Some(command) => command,
            None => tokio::select! {
                _ = stop.changed() => return,
                command = receiver.recv() => match command {
                    Some(command) => command,
                    None => return,
                },
            },
        };
        match command {
            JournalCommand::Append { event, reply } => {
                let mut batch = vec![(event, reply)];
                while batch.len() < batch_size {
                    match receiver.try_recv() {
                        Ok(JournalCommand::Append { event, reply }) => batch.push((event, reply)),
                        Ok(command) => {
                            pending = Some(command);
                            break;
                        }
                        Err(mpsc::error::TryRecvError::Empty) => break,
                        Err(mpsc::error::TryRecvError::Disconnected) => break,
                    }
                }
                let events = batch
                    .iter()
                    .map(|(event, _)| event.clone())
                    .collect::<Vec<_>>();
                match bounded_repository_call(
                    &mut stop,
                    operation_timeout,
                    repository.append_events(&events),
                )
                .await
                {
                    Ok(Ok(())) => {
                        for (_, reply) in batch {
                            let _ = reply.send(Ok(()));
                        }
                    }
                    Ok(Err(error)) => {
                        receiver.close();
                        let code = error.code();
                        let message = error.to_string();
                        for (_, reply) in batch {
                            let _ = reply.send(Err(EventError::History(HistoryError::new(
                                code,
                                message.clone(),
                            ))));
                        }
                        return;
                    }
                    Err(error) => {
                        receiver.close();
                        for (_, reply) in batch {
                            let failure = match error {
                                EventError::JournalOperationTimeout => {
                                    EventError::JournalOperationTimeout
                                }
                                _ => EventError::JournalClosed,
                            };
                            let _ = reply.send(Err(failure));
                        }
                        return;
                    }
                }
            }
            JournalCommand::PutOutput { output, reply } => {
                let result = bounded_repository_call(
                    &mut stop,
                    operation_timeout,
                    repository.put_node_output(output),
                )
                .await;
                if send_repository_result(&mut receiver, reply, result) {
                    return;
                }
            }
            JournalCommand::Finish {
                update,
                event,
                reply,
            } => {
                let result = bounded_repository_call(
                    &mut stop,
                    operation_timeout,
                    repository.finish_run(update, *event),
                )
                .await;
                if send_repository_result(&mut receiver, reply, result) {
                    return;
                }
            }
            JournalCommand::Flush(reply) => {
                let _ = reply.send(Ok(()));
            }
        }
    }
}

async fn bounded_repository_call<T>(
    stop: &mut watch::Receiver<bool>,
    operation_timeout: Duration,
    operation: impl Future<Output = Result<T, HistoryError>>,
) -> Result<Result<T, HistoryError>, EventError> {
    tokio::select! {
        _ = stop.changed() => Err(EventError::JournalClosed),
        result = timeout(operation_timeout, operation) => {
            result.map_err(|_| EventError::JournalOperationTimeout)
        }
    }
}

fn send_repository_result<T>(
    receiver: &mut mpsc::Receiver<JournalCommand>,
    reply: oneshot::Sender<Result<T, EventError>>,
    result: Result<Result<T, HistoryError>, EventError>,
) -> bool {
    match result {
        Ok(Ok(value)) => {
            let _ = reply.send(Ok(value));
            false
        }
        Ok(Err(error)) => {
            receiver.close();
            let _ = reply.send(Err(EventError::History(error)));
            true
        }
        Err(error) => {
            receiver.close();
            let _ = reply.send(Err(error));
            true
        }
    }
}
