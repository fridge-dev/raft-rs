//! This module is a raft-specific commit log that wraps the generic commit log. Right now, this
//! might seem odd. But I plan to move the commitlog mod into its own crate/repo, at which point
//! this abstraction will make more sense.

use crate::commitlog::{Entry, Log, Index};
use crate::replica::local_state::Term;

pub struct CommitLog<L: Log<RaftLogEntry>> {
    // This is the log that we're replicating.
    log: L,
    // Index of highest log entry that we've locally written.
    latest_index: Index,
    // Term when the highest log entry was locally written.
    latest_term: Term,
    // Index of highest log entry known to be committed.
    commit_index: Index,
    // Index of highest log entry applied to state machine.
    last_applied_index: Index, // TODO: is this needed? It is probably the same as `commit_index` in practice
}

impl<L: Log<RaftLogEntry>> CommitLog<L> {
    pub fn new(log: L) -> Self {
        // TODO: properly initialize. This should be `-1`
        let latest_index = log.next_index();

        CommitLog {
            log,
            latest_index,
            latest_term: Term::new(0),
            commit_index: Index::new(0),
            last_applied_index: Index::new(0),
        }
    }

    pub fn latest_entry(&self) -> (Term, Index) {
        (self.latest_term, self.latest_index)
    }
}

#[derive(Clone)]
pub struct RaftLogEntry {
    term: Term,
    data: Vec<u8>,
}

impl Entry for RaftLogEntry {}

impl From<Vec<u8>> for RaftLogEntry {
    fn from(_: Vec<u8>) -> Self {
        unimplemented!()
    }
}

impl Into<Vec<u8>> for RaftLogEntry {
    fn into(self) -> Vec<u8> {
        unimplemented!()
    }
}
