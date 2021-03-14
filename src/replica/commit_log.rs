//! This module is a raft-specific commit log that wraps the generic commit log. Right now, this
//! might seem odd. But I plan to move the commitlog mod into its own crate/repo, at which point
//! this abstraction will make more sense.

use crate::api;
use crate::commitlog::{Entry, Index, Log};
use crate::replica::local_state::Term;
use bytes::Bytes;
use std::io;

/// RaftLog is the raft-specific log facade.
///
/// Note: A log entry has 3 states (not modeled directly in code):
/// 1. Persisted - written to disk, not yet replicated to majority
/// 2. Committed - written to disk, replicated to majority
/// 3. Applied - a committed entry that has also been applied to the state machine
///
/// A log entry's state has no global truth. Each replica will have their own local view of what
/// state the log entry is in.
pub struct RaftLog<L>
where
    L: Log<RaftCommitLogEntry>,
{
    // Application's info/debug log.
    logger: slog::Logger,

    // This is the log that we're replicating.
    log: L,
    // Metadata about the highest log entry that we've locally written. It must be updated atomically.
    latest_entry_metadata: Option<(Term, Index)>,

    // Commit stream to publish committed entries to. To be consumed by the application layer to
    // apply committed entries to their state machine.
    commit_stream: api::CommitStreamPublisher,
    // TODO:1 model as optional.
    // Index of highest log entry known to be committed.
    commit_index: Index,
    // Index of highest log entry applied to state machine.
    last_applied_index: Index,
}

impl<L> RaftLog<L>
where
    L: Log<RaftCommitLogEntry>,
{
    pub fn new(logger: slog::Logger, log: L, commit_stream: api::CommitStreamPublisher) -> Self {
        // TODO:3 properly initialize based on existing log. For now, always assume empty log.
        assert_eq!(log.next_index(), Index::new(1));

        RaftLog {
            logger,
            log,
            latest_entry_metadata: None,
            commit_stream,
            commit_index: Index::new(0),
            last_applied_index: Index::new(0),
        }
    }

    pub fn latest_entry(&self) -> Option<(Term, Index)> {
        self.latest_entry_metadata
    }

    pub fn read(&self, index: Index) -> Result<Option<RaftCommitLogEntry>, io::Error> {
        self.log.read(index)
    }

    /// Remove anything starting at `index` and later.
    pub fn truncate(&mut self, index: Index) -> Result<(), io::Error> {
        let new_latest_entry_index = index.minus(1);
        let new_latest_entry_metadata = self
            .log
            .read(new_latest_entry_index)?
            .map(|latest_entry| (latest_entry.term, new_latest_entry_index));

        // Only update log after we've successfully read what new state will be. I guess
        // reading after updating helps guarantee we can read in the future. Hmm.
        self.log.truncate(index);

        self.latest_entry_metadata = new_latest_entry_metadata;
        Ok(())
    }

    pub fn append(&mut self, entry: RaftCommitLogEntry) -> Result<Index, io::Error> {
        let appended_term = entry.term;
        let appended_index = self.log.append(entry)?;
        // Only update state after log action completes.
        self.latest_entry_metadata = Some((appended_term, appended_index));

        Ok(appended_index)
    }

    pub fn commit_index(&self) -> Index {
        self.commit_index
    }

    pub fn ratchet_fwd_commit_index(&mut self, new_commit_index: Index) {
        if new_commit_index <= self.commit_index {
            panic!(
                "ratchet_fwd_commit_index() called with '{:?}' < current commit index {:?}",
                new_commit_index, self.commit_index
            );
        }

        self.commit_index = new_commit_index;
    }

    /// apply_all_committed_entries applies all committed but unapplied entries in order.
    pub fn apply_all_committed_entries(&mut self) {
        // TODO:2 Make infalliable by buffering in memory all of the uncommited entries.
        if let Err(e) = self.try_apply_all_committed_entries() {
            // We've already persisted the log. Applying committed logs is not on critical
            // path. We can wait to retry next time.
            slog::error!(self.logger, "Failed to apply a log entry. {:?}", e);
        }
    }

    fn try_apply_all_committed_entries(&mut self) -> Result<(), io::Error> {
        while self.last_applied_index < self.commit_index {
            let next_index = self.last_applied_index.plus(1);

            // TODO:2 implement batch/streamed read.
            let entry = match self.read(next_index) {
                Ok(Some(entry)) => entry,
                // TODO:2 validate and/or gracefully handle.
                Ok(None) => panic!("This should never happen #5230185123"),
                Err(e) => return Err(e),
            };

            self.commit_stream.notify_commit(
                &self.logger,
                api::CommittedEntry {
                    key: api::EntryKey {
                        term: entry.term,
                        entry_index: next_index,
                    },
                    data: Bytes::from(entry.data),
                },
            );
            self.last_applied_index = next_index;
        }

        Ok(())
    }
}

/// Byte representation:
///
/// ```text
/// |                                         1                           |
/// | 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 0 | 1 | 2 | 3 | 4 | 5 | ... |
/// +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+-...-+
/// |Vrs|       Term (8 bytes)          |   Data (variable size)      ... |
/// +---+-------------------------------+-----------------------------...-+
/// ```
///
/// * `Vrs` - version of the serialized payload
/// * `Term` - raft leadership term when this entry was created
/// * `Data` - app specific data payload
///
/// Not needed:
///
/// * Checksum is not needed, it's guaranteed by underlying commitlog.
/// * Size/length of `Data` is not needed; the underlying commitlog will give us the correctly
///   allocated array.
///
/// TODO:1.5 is this struct needed as `pub`? I think we can expose these params separately to caller.
#[derive(Clone)]
pub struct RaftCommitLogEntry {
    pub term: Term,
    pub data: Vec<u8>,
}

const RAFT_LOG_ENTRY_FORMAT_VERSION: u8 = 1;

impl Entry for RaftCommitLogEntry {}

impl From<Vec<u8>> for RaftCommitLogEntry {
    fn from(bytes: Vec<u8>) -> Self {
        // TODO:1 research how to do this correctly, safely, and efficiently.
        // TODO:2 use TryFrom so we can return error
        assert!(bytes.len() >= 8);
        assert_eq!(bytes[0], RAFT_LOG_ENTRY_FORMAT_VERSION);

        let term: u64 = bytes[1] as u64
            | (bytes[2] as u64) << 1 * 8
            | (bytes[3] as u64) << 2 * 8
            | (bytes[4] as u64) << 3 * 8
            | (bytes[5] as u64) << 4 * 8
            | (bytes[6] as u64) << 5 * 8
            | (bytes[7] as u64) << 6 * 8
            | (bytes[8] as u64) << 7 * 8;
        let mut data: Vec<u8> = Vec::with_capacity(bytes.len() - 8);
        data.clone_from_slice(&bytes[8..]);
        RaftCommitLogEntry {
            term: Term::new(term),
            data,
        }
    }
}

impl Into<Vec<u8>> for RaftCommitLogEntry {
    fn into(self) -> Vec<u8> {
        let mut bytes: Vec<u8> = Vec::with_capacity(1 + 8 + self.data.len());

        let term = self.term.into_inner();
        bytes.push(RAFT_LOG_ENTRY_FORMAT_VERSION);
        bytes.push(term as u8);
        bytes.push((term >> 1 * 8) as u8);
        bytes.push((term >> 2 * 8) as u8);
        bytes.push((term >> 3 * 8) as u8);
        bytes.push((term >> 4 * 8) as u8);
        bytes.push((term >> 5 * 8) as u8);
        bytes.push((term >> 6 * 8) as u8);
        bytes.push((term >> 7 * 8) as u8);

        for b in self.data {
            bytes.push(b);
        }

        bytes
    }
}
