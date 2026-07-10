use std::sync::Arc;

use tokio::sync::{mpsc, oneshot};

use crate::{
    events::protocol::RunEvent,
    history::{
        repository::RunRepository,
        types::{NodeOutputRecord, TerminalUpdate},
    },
};

use super::hub::EventError;

enum JournalCommand {
    Append(RunEvent),
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

#[derive(Clone)]
pub(crate) struct EventJournal {
    sender: mpsc::Sender<JournalCommand>,
}

impl EventJournal {
    pub(crate) fn new(
        repository: Arc<dyn RunRepository>,
        capacity: usize,
        batch_size: usize,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(capacity.max(1));
        tokio::spawn(run_worker(receiver, repository, batch_size.max(1)));
        Self { sender }
    }

    pub(crate) async fn append(&self, event: RunEvent) -> Result<(), EventError> {
        self.sender
            .send(JournalCommand::Append(event))
            .await
            .map_err(|_| EventError::JournalClosed)
    }

    pub(crate) async fn put_output(&self, output: NodeOutputRecord) -> Result<(), EventError> {
        let (reply, response) = oneshot::channel();
        self.sender
            .send(JournalCommand::PutOutput { output, reply })
            .await
            .map_err(|_| EventError::JournalClosed)?;
        response.await.map_err(|_| EventError::JournalClosed)?
    }

    pub(crate) async fn finish(
        &self,
        update: TerminalUpdate,
        event: RunEvent,
    ) -> Result<bool, EventError> {
        let (reply, response) = oneshot::channel();
        self.sender
            .send(JournalCommand::Finish {
                update,
                event: Box::new(event),
                reply,
            })
            .await
            .map_err(|_| EventError::JournalClosed)?;
        response.await.map_err(|_| EventError::JournalClosed)?
    }

    pub(crate) async fn flush(&self) -> Result<(), EventError> {
        let (reply, response) = oneshot::channel();
        self.sender
            .send(JournalCommand::Flush(reply))
            .await
            .map_err(|_| EventError::JournalClosed)?;
        response.await.map_err(|_| EventError::JournalClosed)?
    }
}

async fn run_worker(
    mut receiver: mpsc::Receiver<JournalCommand>,
    repository: Arc<dyn RunRepository>,
    batch_size: usize,
) {
    let mut pending = None;
    loop {
        let command = match pending.take() {
            Some(command) => command,
            None => match receiver.recv().await {
                Some(command) => command,
                None => return,
            },
        };
        match command {
            JournalCommand::Append(event) => {
                let mut events = vec![event];
                while events.len() < batch_size {
                    match receiver.try_recv() {
                        Ok(JournalCommand::Append(event)) => events.push(event),
                        Ok(command) => {
                            pending = Some(command);
                            break;
                        }
                        Err(mpsc::error::TryRecvError::Empty) => break,
                        Err(mpsc::error::TryRecvError::Disconnected) => break,
                    }
                }
                if repository.append_events(&events).await.is_err() {
                    receiver.close();
                    return;
                }
            }
            JournalCommand::PutOutput { output, reply } => {
                let result = repository
                    .put_node_output(output)
                    .await
                    .map_err(EventError::History);
                let _ = reply.send(result);
            }
            JournalCommand::Finish {
                update,
                event,
                reply,
            } => {
                let result = repository
                    .finish_run(update, *event)
                    .await
                    .map_err(EventError::History);
                let _ = reply.send(result);
            }
            JournalCommand::Flush(reply) => {
                let _ = reply.send(Ok(()));
            }
        }
    }
}
