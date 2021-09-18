use crate::commitlog::Index;
use crate::replica::Term;
use bytes::Bytes;
use tokio::sync::mpsc;

pub(super) struct CommitStreamPublisher {
    sender: mpsc::UnboundedSender<CommittedEntry>,
}

pub(crate) struct CommitStream {
    receiver: mpsc::UnboundedReceiver<CommittedEntry>,
}

pub(crate) struct CommittedEntry {
    pub(crate) term: Term,
    pub(crate) index: Index,
    pub(crate) data: Bytes,
}

pub(super) fn new() -> (CommitStreamPublisher, CommitStream) {
    let (tx, rx) = mpsc::unbounded_channel();

    let applier_sender = CommitStreamPublisher { sender: tx };
    let applier_receiver = CommitStream { receiver: rx };

    (applier_sender, applier_receiver)
}

impl CommitStreamPublisher {
    pub(super) fn notify_commit(&self, logger: &slog::Logger, term: Term, index: Index, data: Bytes) {
        let committed_entry = CommittedEntry { term, index, data };

        if let Err(_) = self.sender.send(committed_entry) {
            slog::warn!(logger, "CommitStream has disconnected.");
        }
    }
}

impl CommitStream {
    pub(crate) async fn recv(&mut self) -> Option<CommittedEntry> {
        self.receiver.recv().await
    }
}
