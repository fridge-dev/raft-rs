use crate::commitlog::Index;
use crate::replica::election::timers::LeaderTimerHandle;
use crate::replica::ReplicaId;
use std::collections::{HashMap, HashSet};

pub(crate) struct LeaderStateTracker {
    peer_state: HashMap<ReplicaId, PeerState>,
}

impl LeaderStateTracker {
    pub(super) fn new(peer_state: HashMap<ReplicaId, PeerState>) -> Self {
        LeaderStateTracker { peer_state }
    }

    pub(crate) fn peer_state_mut(&mut self, peer_id: &ReplicaId) -> Option<&mut PeerState> {
        self.peer_state.get_mut(peer_id)
    }

    pub(crate) fn peer_ids(&self) -> HashSet<ReplicaId> {
        self.peer_state.keys().cloned().collect()
    }

    pub(crate) fn peers_iter(&self) -> impl Iterator<Item = &PeerState> {
        self.peer_state.values()
    }
}

pub(crate) struct PeerState {
    // Held to send heartbeats for this peer
    leader_timer_handler: LeaderTimerHandle,

    // > index of the next log entry to send to that server
    // > (initialized to leader last log index + 1)
    next: Index,
    // > index of highest log entry known to be replicated on server
    // > (initialized to 0, increases monotonically)
    // After initial reconciliation of follower logs, this will converge
    // to always be the same as `next`.
    matched: Option<Index>,

    // SeqNo is a form of a logical clock that tracks a term leader's interactions with a peer. When
    // a replica becomes leader, it initializes last sent/received to 0. Each time leader sends a
    // request, it increments the last sent SeqNo and ensures the response will be associated with
    // that SeqNo. If a leader receives a SeqNo from earlier than a previously received SeqNo, it
    // discards it.
    last_sent_seq_no: u64,
    last_received_seq_no: u64,
}

impl PeerState {
    pub(super) fn new(leader_timer_handler: LeaderTimerHandle, previous_log_entry_index: Option<Index>) -> Self {
        PeerState {
            leader_timer_handler,
            next: previous_log_entry_index
                .map(|i| i.plus(1))
                .unwrap_or_else(|| Index::start_index()),
            matched: None,
            last_sent_seq_no: 0,
            last_received_seq_no: 0,
        }
    }

    pub(crate) fn next_and_previous_log_index(&self) -> (Index, Option<Index>) {
        (self.next, self.next.checked_minus(1))
    }

    pub(crate) fn matched(&self) -> Option<Index> {
        self.matched
    }

    pub(crate) fn handle_append_entries_result(
        &mut self,
        logger: &slog::Logger,
        received_seq_no: u64,
        update: PeerStateUpdate,
    ) {
        if !self.ratchet_fwd_received_seq_no(received_seq_no) {
            slog::warn!(
                logger,
                "Dropping out of date seq-no({:?}): {:?}",
                received_seq_no,
                update
            );
            return;
        }

        match update {
            PeerStateUpdate::OtherError => { /* No action */ }
            PeerStateUpdate::Success {
                previous_log_entry,
                num_entries_replicated,
            } => {
                self.update_log(previous_log_entry, num_entries_replicated);
            }
            PeerStateUpdate::PeerLogBehind => {
                self.rewind_log(logger);
            }
        }
    }

    fn update_log(&mut self, previous_log_entry: Option<Index>, num_entries_replicated: usize) {
        let new_matched = match (previous_log_entry, num_entries_replicated) {
            (_, 0) => {
                // We didn't append any new logs, it was just a heartbeat, so do nothing.
                return;
            }
            (None, n) => Index::new_usize(n),
            (Some(prev), n) => prev.plus(n as u64),
        };
        let new_next = new_matched.plus(1);

        // Panic here, because it means as leader, we either sent something wrong or are tracking state wrong.
        assert!(
            new_next > self.next,
            "Next can only ratchet forward. CurrentNext={:?}, NewNext={:?}",
            self.next,
            new_next
        );
        if let Some(matched) = self.matched {
            assert!(
                new_matched > matched,
                "Matched can only ratchet forward. CurrentMatched={:?}, NewMatched={:?}",
                matched,
                new_matched
            )
        }

        self.next = new_next;
        self.matched.replace(new_matched);
    }

    fn rewind_log(&mut self, logger: &slog::Logger) {
        // Don't panic here, because peer could return garbage data.
        if self.matched.is_some() {
            slog::warn!(
                logger,
                "Illegal state: Can't handle AppendEntries rewind error after any success. Not mutating state."
            );
            return;
        }

        if let Some(new) = self.next.checked_minus(1) {
            self.next = new
        } else {
            slog::warn!(logger, "Can't rewind peer log, already at beginning of log.")
        }
    }

    pub(crate) fn has_outstanding_request(&self) -> bool {
        self.last_received_seq_no < self.last_sent_seq_no
    }

    pub(crate) fn next_seq_no(&mut self) -> u64 {
        self.last_sent_seq_no += 1;
        self.last_sent_seq_no
    }

    /// returns true if the state was mutated.
    fn ratchet_fwd_received_seq_no(&mut self, received_seq_no: u64) -> bool {
        if self.last_received_seq_no < received_seq_no && received_seq_no <= self.last_sent_seq_no {
            self.last_received_seq_no = received_seq_no;
            true
        } else {
            false
        }
    }

    pub(crate) fn reset_heartbeat_timer(&self) {
        self.leader_timer_handler.reset_heartbeat_timer();
    }
}

#[derive(Debug)]
pub(crate) enum PeerStateUpdate {
    Success {
        previous_log_entry: Option<Index>,
        num_entries_replicated: usize,
    },
    PeerLogBehind,
    OtherError,
}
