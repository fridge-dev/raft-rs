//! This module is a raft-specific commit log that wraps the generic commit log. Right now, this
//! might seem odd. But I plan to move the commitlog mod into its own crate/repo, at which point
//! this abstraction will make more sense.

use crate::commitlog::{Entry, Index, Log};
use crate::replica::local_state::Term;
use crate::replica::raft_rpcs::LeaderLogEntry;
use std::io;

pub struct CommitLog<L: Log<RaftLogEntry>> {
    // This is the log that we're replicating.
    log: L,
    // Term when the highest log entry was locally written.
    term_of_latest_entry: Term,
    // Index of highest log entry that we've locally written.
    index_of_latest_entry: Index,

    // TODO:1 move SM, commit+applied index to its own struct. They belong together.
    // Index of highest log entry known to be committed.
    pub commit_index: Index,
    // Index of highest log entry applied to state machine.
    pub last_applied_index: Index,
}

impl<L: Log<RaftLogEntry>> CommitLog<L> {
    pub fn new(log: L) -> Self {
        // TODO:1 properly initialize based on last log entry.
        let index_of_latest_entry = log.next_index();
        let term_of_latest_entry = Term::new(0);

        CommitLog {
            log,
            term_of_latest_entry,
            index_of_latest_entry,
            commit_index: Index::new(0),
            last_applied_index: Index::new(0),
        }
    }

    pub fn latest_entry(&self) -> (Term, Index) {
        (self.term_of_latest_entry, self.index_of_latest_entry)
    }

    pub fn read(&self, index: Index) -> Result<Option<RaftLogEntry>, io::Error> {
        self.log.read(index)
    }

    pub fn next_index(&self) -> Index {
        self.log.next_index()
    }

    // note: unsure about delta type
    pub fn rewind_single_entry(&mut self) {
        // Removes exactly just the latest entry
        self.log.truncate(self.index_of_latest_entry);
        // Decrement index
        self.index_of_latest_entry.decr(1);
        // Update term
        match self.log.read(self.index_of_latest_entry) {
            Ok(Some(entry)) => {
                self.term_of_latest_entry = entry.term;
            }
            Ok(None) => {
                // TODO:3 handle error properly or prevent it from being possible.
                // TODO:3 if can't, validate this is safe.
                self.term_of_latest_entry = Term::new(0);
            }
            Err(_) => {
                // TODO:3 handle error properly or prevent it from being possible.
                // TODO:3 if can't, validate this is safe.
                self.term_of_latest_entry = Term::new(0);
            }
        }
    }

    pub fn append(&mut self, entries: Vec<LeaderLogEntry>) -> Result<(), io::Error> {
        // Pre-condition: index should match.
        if let Some(first) = entries.first() {
            assert_eq!(self.log.next_index(), first.index, "FRICK. This is NOT GOOD. We attempted to append log from leader even though we don't have next log. We are bug!");
        }

        let mut last_appended_index = self.index_of_latest_entry;
        let mut last_appended_term = self.term_of_latest_entry;
        for next_entry in entries {
            // Intermediate Pre-condition(s): index(es) should match.
            assert_eq!(self.log.next_index(), next_entry.index, "FRICK2. This is NOT GOOD. We attempted to append log from leader even though we don't have next log. We are bug!");

            let raft_entry = RaftLogEntry {
                term: next_entry.term,
                data: next_entry.data,
            };
            match self.log.append(raft_entry) {
                Ok(appended_index) => {
                    // Intermediate Post-condition(s): index(es) should match.
                    // Am I just paranoid now? Yes. Damn. This is probably too much.
                    // TODO:2 find better way to safely/statically guarantee invariants. Or maybe just trust in commitlog lib, and validate request at higher level.
                    assert_eq!(appended_index, next_entry.index, "FRICK2. This is NOT GOOD. We attempted to append log from leader even though we don't have next log. We are bug!");
                    last_appended_index = appended_index;
                    last_appended_term = next_entry.term;
                }
                Err(e) => {
                    // Revert log to original state then return err.
                    // TODO:2 validate invariants via post-condition
                    let mut index_to_truncate = self.index_of_latest_entry;
                    index_to_truncate.incr(1);
                    self.log.truncate(index_to_truncate);
                    return Err(e);
                }
            }
        }

        // Only update these once completely successful.
        // TODO:2 validate invariants via post-condition
        self.index_of_latest_entry = last_appended_index;
        self.term_of_latest_entry = last_appended_term;

        Ok(())
    }

    // Does this need to return new entries to apply to SM? Should SM just live within this struct?
    // TODO:1 move this and commit+applied index to its own struct.
    pub fn ratchet_commit_index(&mut self, _commit_index: Index) {
        // TODO:1 impl
    }
}

/// Byte representation:
///
/// ```text
///                                           1
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
pub struct RaftLogEntry {
    pub term: Term,
    pub data: Vec<u8>,
}

const RAFT_LOG_ENTRY_FORMAT_VERSION: u8 = 1;

impl Entry for RaftLogEntry {}

impl From<Vec<u8>> for RaftLogEntry {
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
        RaftLogEntry {
            term: Term::new(term),
            data,
        }
    }
}

impl Into<Vec<u8>> for RaftLogEntry {
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
