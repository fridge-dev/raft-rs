use crate::commitlog;
use crate::commitlog::Index;
use crate::replica::local_state::Term;
use crate::replica::write_ahead_log::commit_stream::CommitStreamPublisher;
use bytes::Bytes;
use std::io;
use crate::replica::WriteAheadLogEntry;

/// RaftLog is the raft-specific log facade.
///
/// Note: A log entry has 3 states (not modeled directly in code):
/// 1. Persisted - written to disk, not yet replicated to majority
/// 2. Committed - written to disk, replicated to majority
/// 3. Applied - a committed entry that has also been applied to the state machine
///
/// A log entry's state has no global truth. Each replica will have their own local view of what
/// state the log entry is in.
pub(in super::super) struct WriteAheadLog<L>
    where
        L: commitlog::Log<WriteAheadLogEntry>,
{
    // Application's info/debug log.
    logger: slog::Logger,

    // This is the log that we're replicating.
    log: L,
    // Metadata about the highest log entry that we've locally written. It must be updated atomically.
    latest_entry_metadata: Option<(Term, Index)>,

    // Commit stream to publish committed entries to. To be consumed by the application layer to
    // apply committed entries to their state machine.
    commit_stream: CommitStreamPublisher,
    // Index of highest log entry known to be committed. None if nothing is committed.
    commit_index: Option<Index>,
    // Index of highest log entry applied to state machine. None if nothing is applied.
    last_applied_index: Option<Index>,
}

impl<L> WriteAheadLog<L>
    where
        L: commitlog::Log<WriteAheadLogEntry>,
{
    pub(super) fn new(logger: slog::Logger, log: L, commit_stream: CommitStreamPublisher) -> Self {
        // TODO:3 properly initialize based on existing log. For now, always assume empty log.
        assert_eq!(
            log.next_index(),
            Index::start_index(),
            "We only know how to handle initialization of an empty log."
        );

        WriteAheadLog {
            logger,
            log,
            latest_entry_metadata: None,
            commit_stream,
            commit_index: None,
            last_applied_index: None,
        }
    }

    pub(crate) fn latest_entry(&self) -> Option<(Term, Index)> {
        self.latest_entry_metadata
    }

    pub(crate) fn read(&self, index: Index) -> Result<Option<WriteAheadLogEntry>, io::Error> {
        self.log.read(index)
    }

    fn read_required(&self, index: Index) -> Result<WriteAheadLogEntry, io::Error> {
        match self.read(index) {
            Ok(Some(entry)) => Ok(entry),
            Ok(None) => panic!("read_required() found no log entry for index {:?}", index),
            Err(ioe) => Err(ioe),
        }
    }

    /// Remove anything starting at `index` and later.
    pub(crate) fn truncate(&mut self, index: Index) -> Result<(), io::Error> {
        let mut new_latest_entry_metadata = None;
        if let Some(new_latest_entry_index) = index.checked_minus(1) {
            new_latest_entry_metadata = self
                .read(new_latest_entry_index)?
                .map(|latest_entry| (latest_entry.term, new_latest_entry_index));
        }

        // Only update log after we've successfully read what new state will be. On the other hand,
        // I guess reading after updating helps guarantee we can read in the future. Hmm.
        self.log.truncate(index);

        self.latest_entry_metadata = new_latest_entry_metadata;
        Ok(())
    }

    pub(crate) fn append(&mut self, entry: WriteAheadLogEntry) -> Result<Index, io::Error> {
        let appended_term = entry.term;
        let appended_index = self.log.append(entry)?;
        // Only update state after log action completes.
        self.latest_entry_metadata = Some((appended_term, appended_index));

        Ok(appended_index)
    }

    pub(crate) fn commit_index(&self) -> Option<Index> {
        self.commit_index
    }

    pub(crate) fn ratchet_fwd_commit_index_if_valid(
        &mut self,
        tentative_new_commit_index: Index,
        current_term: Term,
    ) -> Result<(), io::Error> {
        // Gracefully handle only the case where commit index is unchanged. Panic (later) if index
        // is decreasing.
        if let Some(current_commit_index) = self.commit_index {
            if tentative_new_commit_index == current_commit_index {
                return Ok(());
            }
        }

        // > If there exists an N such that N > commitIndex, a majority
        // > of matchIndex[i] ≥ N, and log[N].term == currentTerm:
        // > set commitIndex = N (§5.3, §5.4).
        let entry = self.read_required(tentative_new_commit_index)?;
        if entry.term != current_term {
            return Ok(());
        }

        self.ratchet_fwd_commit_index_panicking(tentative_new_commit_index);

        Ok(())
    }

    pub(crate) fn ratchet_fwd_commit_index_if_changed(&mut self, new_commit_index: Index) {
        // Gracefully handle unchanged. Panic (later) if index is decreasing.
        if matches!(self.commit_index(), Some(ci) if ci == new_commit_index) {
            return;
        }

        self.ratchet_fwd_commit_index_panicking(new_commit_index);
    }

    fn ratchet_fwd_commit_index_panicking(&mut self, new_commit_index: Index) {
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
    pub(crate) fn apply_all_committed_entries(&mut self) {
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
        // TODO:2.5 implement batch/streamed read.
        // TODO:3 Make failure less likely by buffering in memory limited number of recent uncommited entries.
        // TODO:2.5 validate and/or gracefully handle.
        let entry = self.read_required(index_to_apply)?;

        self.commit_stream
            .notify_commit(&self.logger, entry.term, index_to_apply, Bytes::from(entry.data));

        Ok(())
    }
}
