use crate::api::types::RaftEntryId;
use crate::commitlog::Index;
use crate::replica::Term;
use bytes::Bytes;
use std::fmt;
use tokio::sync::mpsc;

pub(crate) fn new() -> (RaftCommitStreamPublisher, RaftCommitStream) {
    let (tx, rx) = mpsc::unbounded_channel();

    let applier_sender = RaftCommitStreamPublisher { sender: tx };
    let applier_receiver = RaftCommitStream { receiver: rx };

    (applier_sender, applier_receiver)
}

pub(crate) struct RaftCommitStreamPublisher {
    sender: mpsc::UnboundedSender<RaftCommittedEntry>,
}

// For external application to call into this library.
pub struct RaftCommitStream {
    receiver: mpsc::UnboundedReceiver<RaftCommittedEntry>,
}

pub struct RaftCommittedEntry {
    pub entry_id: RaftEntryId,
    pub data: Bytes,
}

impl RaftCommitStreamPublisher {
    pub(crate) fn notify_commit(&self, logger: &slog::Logger, term: Term, entry_index: Index, data: Bytes) {
        let entry = RaftCommittedEntry {
            entry_id: RaftEntryId { term, entry_index },
            data,
        };
        if let Err(_) = self.sender.send(entry) {
            slog::warn!(logger, "CommitStream has disconnected.");
        }
    }
}

impl fmt::Debug for RaftCommittedEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CommittedEntry")
            .field("entry_id", &self.entry_id)
            .field("data.len()", &self.data.len())
            .finish()
    }
}

impl RaftCommitStream {
    // TODO:3 How to handle when we accepted entry for repl, then lost leadership?
    //        Do we just *not* inform app layer?
    /// `next_entry()` returns the next committed entry to be applied to your application's state
    /// machine. It will return None when the commit stream has been terminally closed because the
    /// handle to the ReplicatedLog has been dropped by your application.
    pub async fn next_entry(&mut self) -> Option<RaftCommittedEntry> {
        self.receiver.recv().await
    }
}
