use crate::EntryKey;
use bytes::Bytes;
use tokio::sync::mpsc;

pub fn create_commit_stream() -> (CommitStreamPublisher, CommitStream) {
    let (tx, rx) = mpsc::unbounded_channel();

    let applier_sender = CommitStreamPublisher { sender: tx };
    let applier_receiver = CommitStream { receiver: rx };

    (applier_sender, applier_receiver)
}

pub struct CommitStreamPublisher {
    sender: mpsc::UnboundedSender<CommittedEntry>,
}

impl CommitStreamPublisher {
    pub fn notify_commit(&self, logger: &slog::Logger, result: CommittedEntry) {
        if let Err(_) = self.sender.send(result) {
            slog::warn!(logger, "CommitStream has disconnected.");
        }
    }
}

// For external application to call into this library.
pub struct CommitStream {
    receiver: mpsc::UnboundedReceiver<CommittedEntry>,
}

pub struct CommittedEntry {
    pub key: EntryKey,
    pub data: Bytes,
}

impl CommitStream {
    // TODO:3 How to handle when we accepted entry for repl, then lost leadership?
    //        Do we just *not* inform app layer?
    /// next returns the next committed entry to be applied to your application's state machine.
    pub async fn next(&mut self) -> CommittedEntry {
        self.receiver
            .recv()
            .await
            .expect("Replica event loop should never exit.")
    }
}
