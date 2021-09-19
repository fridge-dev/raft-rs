//! This module is a raft-specific commit log that wraps the generic commit log. Right now, this
//! might seem odd. But I plan to move the commitlog mod into its own crate/repo, at which point
//! this abstraction will make more sense.

mod commit_stream;
mod log;
mod log_entry;
mod wiring;

pub(crate) use commit_stream::CommitStream;
pub(crate) use commit_stream::CommittedEntry;
pub(crate) use log_entry::WriteAheadLogEntry;

pub(super) use log::WriteAheadLog;
pub(super) use wiring::wired;
