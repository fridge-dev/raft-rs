//! This module is a raft-specific commit log that wraps the generic commit log. Right now, this
//! might seem odd. But I plan to move the commitlog mod into its own crate/repo, at which point
//! this abstraction will make more sense.

// use crate::commitlog::Log;
//
// pub struct CommitLog<L: Log> {
//     log: L,
// }

use crate::commitlog::Entry;
use crate::replica::replica::Term;

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
