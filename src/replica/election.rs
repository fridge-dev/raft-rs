use crate::actor;
use crate::commitlog::Index;
use crate::replica::peers::ReplicaId;
use crate::replica::timers::{FollowerTimerHandle, LeaderTimerHandle};
use crate::replica::Term;
use std::collections::hash_map::Values;
use std::collections::{HashMap, HashSet};
use std::fmt;
use tokio::sync::watch;
use tokio::time::Duration;

#[derive(Clone)]
pub struct ElectionConfig {
    pub my_replica_id: ReplicaId,
    pub leader_heartbeat_duration: Duration,
    pub follower_min_timeout: Duration,
    pub follower_max_timeout: Duration,
}

/// ElectionState is responsible for holding state specific to the stage in an election. Its
/// methods are responsible for "what" to do. It is NOT responsible for validating anything
/// specific to logs, terms, peers, etc. or knowing "when" to do something.
pub struct ElectionState {
    state: State,
    config: ElectionConfig,
    actor_client: actor::WeakActorClient,
    state_change_notifier: ElectionStateChangeNotifier,
}

impl ElectionState {
    /// `new_follower()` creates a new ElectionState instance that starts out as a follower.
    pub fn new_follower(
        config: ElectionConfig,
        actor_client: actor::WeakActorClient,
    ) -> (Self, ElectionStateChangeListener) {
        let initial_state = State::Follower(FollowerState::new(
            config.follower_min_timeout,
            config.follower_max_timeout,
            actor_client.clone(),
        ));
        let (notifier, listener) = new_event_bus(Self::current_state_impl(&initial_state));

        let election_state = ElectionState {
            state: initial_state,
            config,
            actor_client,
            state_change_notifier: notifier,
        };

        (election_state, listener)
    }

    pub fn transition_to_follower(&mut self, new_leader_id: Option<ReplicaId>) {
        self.state = State::Follower(FollowerState::with_leader_info(
            new_leader_id,
            self.config.follower_min_timeout,
            self.config.follower_max_timeout,
            self.actor_client.clone(),
        ));
        self.notify_new_state();
    }

    pub fn transition_to_candidate_and_vote_for_self(&mut self) {
        let mut cs = CandidateState::new(
            self.config.follower_min_timeout,
            self.config.follower_max_timeout,
            self.actor_client.clone(),
        );

        // Vote for self
        cs.add_received_vote(self.config.my_replica_id.clone());

        self.state = State::Candidate(cs);
        self.notify_new_state();
    }

    pub fn transition_to_leader(
        &mut self,
        term: Term,
        peer_ids: HashSet<ReplicaId>,
        previous_log_entry_index: Option<Index>,
    ) {
        self.state = State::Leader(LeaderState::new(
            peer_ids,
            previous_log_entry_index,
            self.config.leader_heartbeat_duration,
            self.actor_client.clone(),
            term,
        ));
        self.notify_new_state();
    }

    pub fn current_state(&self) -> ElectionStateSnapshot {
        Self::current_state_impl(&self.state)
    }

    fn current_state_impl(state: &State) -> ElectionStateSnapshot {
        match state {
            State::Leader(_) => ElectionStateSnapshot::Leader,
            State::Candidate(_) => ElectionStateSnapshot::Candidate,
            State::Follower(FollowerState { leader_id: None, .. }) => ElectionStateSnapshot::FollowerNoLeader,
            State::Follower(FollowerState {
                leader_id: Some(leader_id),
                ..
            }) => ElectionStateSnapshot::Follower(leader_id.clone()),
        }
    }

    fn notify_new_state(&self) {
        self.state_change_notifier
            .notify_new_state(Self::current_state_impl(&self.state));
    }

    pub fn reset_timeout_if_follower(&self) {
        if let State::Follower(fs) = &self.state {
            fs.reset_timeout();
        }
    }

    pub fn set_leader_if_unknown(&mut self, leader_id: &ReplicaId) {
        if let State::Follower(fs) = &mut self.state {
            if fs.leader_id.is_none() {
                fs.leader_id.replace(leader_id.clone());
                self.notify_new_state();
            }
        }
    }

    /// Return number of votes received if candidate, or None if no longer Candidate.
    pub fn add_vote_if_candidate(&mut self, vote_from: ReplicaId) -> Option<usize> {
        if let State::Candidate(cs) = &mut self.state {
            Some(cs.add_received_vote(vote_from))
        } else {
            None
        }
    }

    pub fn leader_state_mut(&mut self) -> Option<&mut LeaderStateTracker> {
        if let State::Leader(ls) = &mut self.state {
            Some(&mut ls.tracker)
        } else {
            None
        }
    }
}

impl fmt::Debug for ElectionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.state {
            State::Leader(_) => write!(f, "Leader"),
            State::Candidate(_) => write!(f, "Candidate"),
            State::Follower(FollowerState {
                leader_id: Some(leader_id),
                ..
            }) => write!(f, "Follower(Leader={:?})", leader_id),
            State::Follower(FollowerState { leader_id: None, .. }) => write!(f, "Follower(Leader=None)"),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ElectionStateSnapshot {
    Leader,
    Candidate,
    Follower(ReplicaId),
    FollowerNoLeader,
}

enum State {
    Leader(LeaderState),
    Candidate(CandidateState),
    Follower(FollowerState),
}

struct LeaderState {
    tracker: LeaderStateTracker,
}

struct CandidateState {
    received_votes_from: HashSet<ReplicaId>,
    _follower_timeout_tracker: FollowerTimerHandle,
}

struct FollowerState {
    leader_id: Option<ReplicaId>,
    follower_timeout_tracker: FollowerTimerHandle,
}

impl LeaderState {
    pub fn new(
        peer_ids: HashSet<ReplicaId>,
        previous_log_entry_index: Option<Index>,
        heartbeat_duration: Duration,
        actor_client: actor::WeakActorClient,
        term: Term,
    ) -> Self {
        let mut peer_state = HashMap::with_capacity(peer_ids.len());
        for peer_id in peer_ids {
            // TODO:3 eagerly broadcast AE from this task for the initial round.
            let leader_timer_handle =
                LeaderTimerHandle::spawn_timer_task(heartbeat_duration, actor_client.clone(), peer_id.clone(), term);
            peer_state.insert(peer_id, PeerState::new(leader_timer_handle, previous_log_entry_index));
        }

        LeaderState {
            tracker: LeaderStateTracker::new(peer_state),
        }
    }
}

impl CandidateState {
    pub fn new(min_timeout: Duration, max_timeout: Duration, actor_client: actor::WeakActorClient) -> Self {
        CandidateState {
            received_votes_from: HashSet::with_capacity(3),
            _follower_timeout_tracker: FollowerTimerHandle::spawn_timer_task(min_timeout, max_timeout, actor_client),
        }
    }

    /// `add_received_vote()` returns the number of unique votes we've received after adding the
    /// provided `vote_from`
    pub fn add_received_vote(&mut self, vote_from: ReplicaId) -> usize {
        self.received_votes_from.insert(vote_from);
        self.received_votes_from.len()
    }
}

impl FollowerState {
    pub fn new(min_timeout: Duration, max_timeout: Duration, actor_client: actor::WeakActorClient) -> Self {
        FollowerState {
            leader_id: None,
            follower_timeout_tracker: FollowerTimerHandle::spawn_timer_task(min_timeout, max_timeout, actor_client),
        }
    }

    pub fn with_leader_info(
        leader_id: Option<ReplicaId>,
        min_timeout: Duration,
        max_timeout: Duration,
        actor_client: actor::WeakActorClient,
    ) -> Self {
        FollowerState {
            leader_id,
            follower_timeout_tracker: FollowerTimerHandle::spawn_timer_task(min_timeout, max_timeout, actor_client),
        }
    }

    pub fn reset_timeout(&self) {
        self.follower_timeout_tracker.reset_timeout();
    }
}

// ------- Event bus -------

fn new_event_bus(initial_state: ElectionStateSnapshot) -> (ElectionStateChangeNotifier, ElectionStateChangeListener) {
    let (snd, rcv) = watch::channel(initial_state);

    (ElectionStateChangeNotifier { snd }, ElectionStateChangeListener { rcv })
}

struct ElectionStateChangeNotifier {
    snd: watch::Sender<ElectionStateSnapshot>,
}

impl ElectionStateChangeNotifier {
    pub fn notify_new_state(&self, new_state: ElectionStateSnapshot) {
        let _ = self.snd.send(new_state);
    }
}

#[derive(Clone)]
pub struct ElectionStateChangeListener {
    rcv: watch::Receiver<ElectionStateSnapshot>,
}

impl ElectionStateChangeListener {
    pub async fn next(&mut self) -> Option<ElectionStateSnapshot> {
        match self.rcv.changed().await {
            Ok(_) => Some(self.rcv.borrow().clone()),
            Err(_) => None,
        }
    }
}

// ------- Leader state tracking -------

pub struct LeaderStateTracker {
    peer_state: HashMap<ReplicaId, PeerState>,
}

impl LeaderStateTracker {
    fn new(peer_state: HashMap<ReplicaId, PeerState>) -> Self {
        LeaderStateTracker { peer_state }
    }

    pub fn peer_state_mut(&mut self, peer_id: &ReplicaId) -> Option<&mut PeerState> {
        self.peer_state.get_mut(peer_id)
    }

    pub fn peer_ids(&self) -> HashSet<ReplicaId> {
        self.peer_state.keys().cloned().collect()
    }

    pub fn peers_iter(&self) -> Values<'_, ReplicaId, PeerState> {
        self.peer_state.values()
    }
}

pub struct PeerState {
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
    fn new(leader_timer_handler: LeaderTimerHandle, previous_log_entry_index: Option<Index>) -> Self {
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

    pub fn next_and_previous_log_index(&self) -> (Index, Option<Index>) {
        (self.next, self.next.checked_minus(1))
    }

    pub fn matched(&self) -> Option<Index> {
        self.matched
    }

    pub fn handle_append_entries_result(
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

    pub fn has_outstanding_request(&self) -> bool {
        self.last_received_seq_no < self.last_sent_seq_no
    }

    pub fn next_seq_no(&mut self) -> u64 {
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

    pub fn reset_heartbeat_timer(&self) {
        self.leader_timer_handler.reset_heartbeat_timer();
    }
}

#[derive(Debug)]
pub enum PeerStateUpdate {
    Success {
        previous_log_entry: Option<Index>,
        num_entries_replicated: usize,
    },
    PeerLogBehind,
    OtherError,
}
