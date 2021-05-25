use crate::EntryId;
use bytes::Bytes;
use std::fmt;
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
    pub entry_id: EntryId,
    pub data: Bytes,
}

impl fmt::Debug for CommittedEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CommittedEntry")
            .field("entry_id", &self.entry_id)
            .field("data.len()", &self.data.len())
            .finish()
    }
}

impl CommitStream {
    // TODO:3 How to handle when we accepted entry for repl, then lost leadership?
    //        Do we just *not* inform app layer?
    /// `next_entry()` returns the next committed entry to be applied to your application's state
    /// machine. It will return None when the commit stream has been terminally closed because the
    /// handle to the ReplicatedLog has been dropped by your application.
    pub async fn next_entry(&mut self) -> Option<CommittedEntry> {
        self.receiver.recv().await
    }
}
