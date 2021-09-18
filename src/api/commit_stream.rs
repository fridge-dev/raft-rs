use crate::api::types::RaftEntryId;
use crate::replica;
use bytes::Bytes;
use std::fmt;

// For external application to call into this library.
pub struct RaftCommitStream {
    inner: replica::CommitStream,
}

impl RaftCommitStream {
    pub(super) fn new(inner: replica::CommitStream) -> Self {
        Self { inner }
    }

    // TODO:3 How to handle when we accepted entry for repl, then lost leadership?
    //        Do we just *not* inform app layer?
    //
    /// `next_entry()` returns the next committed entry to be applied to your application's state
    /// machine. It will return None when the commit stream has been terminally closed because the
    /// handle to the ReplicatedLog has been dropped by your application.
    pub async fn next_entry(&mut self) -> Option<RaftCommittedEntry> {
        self.inner.recv().await.map(RaftCommittedEntry::from)
    }
}

pub struct RaftCommittedEntry {
    pub entry_id: RaftEntryId,
    pub data: Bytes,
}

impl fmt::Debug for RaftCommittedEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RaftCommittedEntry")
            .field("entry_id", &self.entry_id)
            .field("data.len()", &self.data.len())
            .finish()
    }
}

impl From<replica::CommittedEntry> for RaftCommittedEntry {
    fn from(internal_entry: replica::CommittedEntry) -> Self {
        RaftCommittedEntry {
            entry_id: RaftEntryId {
                term: internal_entry.term,
                entry_index: internal_entry.index,
            },
            data: internal_entry.data,
        }
    }
}
