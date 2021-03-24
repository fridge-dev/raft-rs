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
    // Index of highest log entry known to be committed. None if nothing is committed.
    commit_index: Option<Index>,
    // Index of highest log entry applied to state machine. None if nothing is applied.
    last_applied_index: Option<Index>,
}

impl<L> RaftLog<L>
where
    L: Log<RaftCommitLogEntry>,
{
    pub fn new(logger: slog::Logger, log: L, commit_stream: api::CommitStreamPublisher) -> Self {
        // TODO:3 properly initialize based on existing log. For now, always assume empty log.
        assert_eq!(
            log.next_index(),
            Index::new(1),
            "We only know how to handle initialization of an empty log."
        );

        RaftLog {
            logger,
            log,
            latest_entry_metadata: None,
            commit_stream,
            commit_index: None,
            last_applied_index: None,
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
        let new_latest_entry_index = index.minus(1); // TODO:1.5 panic risk, fix it
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

    pub fn commit_index(&self) -> Option<Index> {
        self.commit_index
    }

    pub fn ratchet_fwd_commit_index(&mut self, new_commit_index: Index) {
        // Assert we only ratchet commit index forward.
        if let Some(current_commit_index) = self.commit_index {
            assert!(
                new_commit_index > current_commit_index,
                "Can't ratchet commit index backwards. Expected [input] {:?} > {:?} [current]",
                new_commit_index,
                current_commit_index,
            );
        }

        // Assert we only mark as committed if we have the entry locally.
        let latest_locally_written_index = self
            .latest_entry_metadata
            .expect("Can't ratchet commit index forward if we don't have any local logs")
            .1;
        assert!(
            latest_locally_written_index >= new_commit_index,
            "Can't ratchet commit index forwards past our local log. Expected [latest log] {:?} > {:?} [input]",
            latest_locally_written_index,
            new_commit_index,
        );

        self.commit_index.replace(new_commit_index);
    }

    /// apply_all_committed_entries applies all committed but unapplied entries in order.
    pub fn apply_all_committed_entries(&mut self) {
        if let Err(e) = self.try_apply_all_committed_entries() {
            // We've already persisted the log. Applying committed logs is not on critical
            // path. We can wait to retry next time.
            slog::error!(self.logger, "Failed to apply a log entry. {:?}", e);
        }
    }

    fn try_apply_all_committed_entries(&mut self) -> Result<(), io::Error> {
        if let Some(commit_index) = self.commit_index {
            // Apply first entry, if not applied
            if self.last_applied_index == None {
                self.apply_single_entry(Index::start_index())?;
                self.last_applied_index.replace(Index::start_index());
            }

            // Guaranteed to be present based on above block.
            let mut last_applied_index = self.last_applied_index.unwrap();

            // Apply all committed indexes until applied catches up.
            //
            // This may be a long running loop, and starve the Replica event loop from handling
            // another event. Given this path is for followers only, it would only be on the
            // critical path for redirecting caller or taking over leadership. Both should rarely
            // overlap with when a new follower is catching up on commits.
            while last_applied_index < commit_index {
                let next_index = last_applied_index.plus(1);
                self.apply_single_entry(next_index)?;
                last_applied_index = next_index;
                self.last_applied_index.replace(last_applied_index);
            }
        }

        Ok(())
    }

    fn apply_single_entry(&mut self, index_to_apply: Index) -> Result<(), io::Error> {
        // TODO:2 implement batch/streamed read.
        // TODO:3 Make failure less likely by buffering in memory limited number of recent uncommited entries.
        let entry = match self.read(index_to_apply) {
            Ok(Some(entry)) => entry,
            // TODO:2 validate and/or gracefully handle.
            Ok(None) => panic!("This should never happen #5230185123"),
            Err(e) => return Err(e),
        };

        self.commit_stream.notify_commit(
            &self.logger,
            api::CommittedEntry {
                data: Bytes::from(entry.data),
                key: api::EntryKey {
                    term: entry.term,
                    entry_index: index_to_apply,
                },
            },
        );

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

        let term = self.term.as_u64();
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
