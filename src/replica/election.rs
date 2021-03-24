use crate::actor;
use crate::commitlog::Index;
use crate::replica::peers::ReplicaId;
use crate::replica::timers::{FollowerTimerHandle, LeaderTimerHandle};
use crate::replica::Term;
use std::collections::{HashMap, HashSet};
use std::fmt;
use tokio::time::Duration;

#[derive(Clone)]
pub struct ElectionConfig {
    pub my_replica_id: ReplicaId,
    pub leader_heartbeat_duration: Duration,
    pub follower_min_timeout: Duration,
    pub follower_max_timeout: Duration,
}

pub struct ElectionState {
    state: State,
    config: ElectionConfig,
    actor_client: actor::ActorClient,
}

impl ElectionState {
    /// `new_follower()` creates a new ElectionState instance that starts out as a follower.
    pub fn new_follower(config: ElectionConfig, actor_client: actor::ActorClient) -> Self {
        ElectionState {
            state: State::Follower(FollowerState::new(
                config.follower_min_timeout,
                config.follower_max_timeout,
                actor_client.clone(),
            )),
            config,
            actor_client,
        }
    }

    pub fn transition_to_follower(&mut self, new_leader_id: Option<ReplicaId>) {
        self.state = State::Follower(FollowerState::with_leader_info(
            new_leader_id,
            self.config.follower_min_timeout,
            self.config.follower_max_timeout,
            self.actor_client.clone(),
        ));
    }

    pub fn transition_to_candidate(&mut self, term: Term, num_voting_replicas: usize) {
        let mut cs = CandidateState::new(
            term,
            num_voting_replicas,
            self.config.follower_min_timeout,
            self.config.follower_max_timeout,
            self.actor_client.clone(),
        );

        // Vote for self
        cs.add_received_vote(self.config.my_replica_id.clone());

        self.state = State::Candidate(cs);
    }

    pub fn transition_to_leader(&mut self, peer_ids: HashSet<ReplicaId>, previous_log_entry_index: Option<Index>) {
        self.state = State::Leader(LeaderState::new(
            peer_ids,
            previous_log_entry_index,
            self.config.leader_heartbeat_duration,
            self.actor_client.clone(),
        ));
    }

    pub fn current_leader(&self) -> CurrentLeader {
        match &self.state {
            State::Leader(_) => CurrentLeader::Me,
            State::Candidate(_) => CurrentLeader::Unknown,
            State::Follower(FollowerState { leader_id: None, .. }) => CurrentLeader::Unknown,
            State::Follower(FollowerState {
                leader_id: Some(leader_id),
                ..
            }) => CurrentLeader::Other(leader_id.clone()),
        }
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
            }
        }
    }

    /// Return true if we've received a majority of votes.
    pub fn add_vote_if_candidate(
        &mut self,
        logger: &slog::Logger,
        term: Term,
        vote_from: ReplicaId,
    ) -> bool {
        if let State::Candidate(cs) = &mut self.state {
            if cs.term != term {
                slog::info!(
                    logger,
                    "Received vote for outdated term {:?}, current term: {:?}.",
                    term,
                    cs.term
                );
                return false;
            }

            let num_votes_received = cs.add_received_vote(vote_from);
            slog::info!(
                logger,
                "Received {}/{} votes for term {:?}",
                num_votes_received,
                cs.num_voting_replicas,
                term
            );
            return num_votes_received >= Self::get_majority_count(cs.num_voting_replicas);
        } else {
            slog::info!(
                logger,
                "Received vote for term {:?} after transitioning to a different election state.",
                term
            );
            return false;
        }
    }

    fn get_majority_count(num_voting_replicas: usize) -> usize {
        (num_voting_replicas / 2) + 1
    }

    pub fn is_currently_candidate_for_term(&self, term: Term) -> bool {
        if let State::Candidate(cs) = &self.state {
            cs.term == term
        } else {
            false
        }
    }

    pub fn leader_state(&self) -> Option<&LeaderStateTracker> {
        if let State::Leader(ls) = &self.state {
            Some(&ls.tracker)
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
            State::Candidate(cs) => write!(f, "Candidate(Term={:?})", cs.term),
            State::Follower(FollowerState {
                leader_id: Some(leader_id),
                ..
            }) => write!(f, "Follower(Leader={:?})", leader_id),
            State::Follower(FollowerState { leader_id: None, .. }) => write!(f, "Follower(Leader=None)"),
        }
    }
}

#[derive(Eq, PartialEq)]
pub enum CurrentLeader {
    Me,
    Other(ReplicaId),
    Unknown,
}

enum State {
    Leader(LeaderState),
    Candidate(CandidateState),
    Follower(FollowerState),
}

struct LeaderState {
    tracker: LeaderStateTracker,
    _heartbeat_timer: LeaderTimerHandle,
}

struct CandidateState {
    term: Term,
    num_voting_replicas: usize,
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
        actor_client: actor::ActorClient,
    ) -> Self {
        let mut peer_state = HashMap::with_capacity(peer_ids.len());
        for peer_id in peer_ids {
            peer_state.insert(peer_id, PeerLogState::new(previous_log_entry_index));
        }

        LeaderState {
            tracker: LeaderStateTracker::new(peer_state),
            _heartbeat_timer: LeaderTimerHandle::spawn_background_task(heartbeat_duration, actor_client),
        }
    }
}

impl CandidateState {
    pub fn new(
        term: Term,
        num_voting_replicas: usize,
        min_timeout: Duration,
        max_timeout: Duration,
        actor_client: actor::ActorClient,
    ) -> Self {
        CandidateState {
            term,
            num_voting_replicas,
            received_votes_from: HashSet::with_capacity(num_voting_replicas),
            _follower_timeout_tracker: FollowerTimerHandle::spawn_background_task(
                min_timeout,
                max_timeout,
                actor_client,
            ),
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
    pub fn new(min_timeout: Duration, max_timeout: Duration, actor_client: actor::ActorClient) -> Self {
        FollowerState {
            leader_id: None,
            follower_timeout_tracker: FollowerTimerHandle::spawn_background_task(
                min_timeout,
                max_timeout,
                actor_client,
            ),
        }
    }

    pub fn with_leader_info(
        leader_id: Option<ReplicaId>,
        min_timeout: Duration,
        max_timeout: Duration,
        actor_client: actor::ActorClient,
    ) -> Self {
        FollowerState {
            leader_id,
            follower_timeout_tracker: FollowerTimerHandle::spawn_background_task(
                min_timeout,
                max_timeout,
                actor_client,
            ),
        }
    }

    pub fn reset_timeout(&self) {
        self.follower_timeout_tracker.reset_timeout();
    }
}

// ------- Leader state tracking -------

pub struct LeaderStateTracker {
    peer_state: HashMap<ReplicaId, PeerLogState>,
}

impl LeaderStateTracker {
    fn new(peer_state: HashMap<ReplicaId, PeerLogState>) -> Self {
        LeaderStateTracker {
            peer_state,
        }
    }

    pub fn peer_state(&self, peer_id: &ReplicaId) -> Option<&PeerLogState> {
        self.peer_state.get(peer_id)
    }

    pub fn peer_state_mut(&mut self, peer_id: &ReplicaId) -> Option<&mut PeerLogState> {
        self.peer_state.get_mut(peer_id)
    }

    pub fn peer_ids(&self) -> HashSet<ReplicaId> {
        self.peer_state.keys().cloned().collect()
    }
}

pub struct PeerLogState {
    // > index of the next log entry to send to that server
    // > (initialized to leader last log index + 1)
    next: Index,
    // > index of highest log entry known to be replicated on server
    // > (initialized to 0, increases monotonically)
    matched: Option<Index>,
}

impl PeerLogState {
    fn new(previous_log_entry_index: Option<Index>) -> Self {
        PeerLogState {
            next: previous_log_entry_index.map(|i| i.plus(1)).unwrap_or_else(|| Index::start_index()),
            matched: None,
        }
    }

    pub fn next_and_previous_index(&self) -> (Index, Option<Index>) {
        (self.next, self.next.checked_minus(1))
    }
}
