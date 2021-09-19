use crate::actor::WeakActorClient;
use crate::commitlog::Index;
use crate::replica::election::state_change_listener::ElectionStateChangeNotifier;
use crate::replica::election::timers::{FollowerTimerHandle, LeaderTimerHandle};
use crate::replica::election::{state_change_listener, LeaderStateTracker, PeerState};
use crate::replica::{ElectionStateChangeListener, ElectionStateSnapshot, LeaderRedirectInfo, ReplicaId, Term};
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::time::Duration;

#[derive(Clone)]
pub(crate) struct ElectionConfig {
    pub my_replica_id: ReplicaId,
    pub leader_heartbeat_duration: Duration,
    pub follower_min_timeout: Duration,
    pub follower_max_timeout: Duration,
}

/// ElectionState is responsible for holding state specific to the stage in an election. Its
/// methods are responsible for "what" to do. It is NOT responsible for validating anything
/// specific to logs, terms, peers, etc. or knowing "when" to do something.
pub(crate) struct ElectionState {
    state: State,
    config: ElectionConfig,
    actor_client: WeakActorClient,
    state_change_notifier: ElectionStateChangeNotifier,
}

impl ElectionState {
    /// `new_follower()` creates a new ElectionState instance that starts out as a follower.
    pub(crate) fn new_follower(
        config: ElectionConfig,
        actor_client: WeakActorClient,
    ) -> (Self, ElectionStateChangeListener) {
        let initial_state = State::Follower(FollowerState::new(
            config.follower_min_timeout,
            config.follower_max_timeout,
            actor_client.clone(),
        ));
        let (notifier, listener) = state_change_listener::new(Self::current_state_impl(&initial_state));

        let election_state = Self {
            state: initial_state,
            config,
            actor_client,
            state_change_notifier: notifier,
        };

        (election_state, listener)
    }

    pub(crate) fn transition_to_follower(&mut self, new_leader: Option<LeaderRedirectInfo>) {
        self.state = State::Follower(FollowerState::with_leader_info(
            new_leader,
            self.config.follower_min_timeout,
            self.config.follower_max_timeout,
            self.actor_client.clone(),
        ));
        self.notify_new_state();
    }

    pub(crate) fn transition_to_candidate_and_vote_for_self(&mut self) {
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

    pub(crate) fn transition_to_leader(
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

    pub(crate) fn current_state(&self) -> ElectionStateSnapshot {
        Self::current_state_impl(&self.state)
    }

    fn current_state_impl(state: &State) -> ElectionStateSnapshot {
        match state {
            State::Leader(_) => ElectionStateSnapshot::Leader,
            State::Candidate(_) => ElectionStateSnapshot::Candidate,
            State::Follower(FollowerState { leader: None, .. }) => ElectionStateSnapshot::FollowerNoLeader,
            State::Follower(FollowerState {
                leader: Some(leader_info),
                ..
            }) => ElectionStateSnapshot::Follower(leader_info.clone()),
        }
    }

    fn notify_new_state(&self) {
        self.state_change_notifier
            .notify_new_state(Self::current_state_impl(&self.state));
    }

    pub(crate) fn reset_timeout_if_follower(&self) {
        if let State::Follower(fs) = &self.state {
            fs.reset_timeout();
        }
    }

    // TODO:3 learn about Cow and consider using Cow<LeaderRedirectInfo>
    pub(crate) fn set_leader_if_unknown(&mut self, leader: &LeaderRedirectInfo) {
        if let State::Follower(fs) = &mut self.state {
            if fs.leader.is_none() {
                fs.leader.replace(leader.clone());
                self.notify_new_state();
            }
        }
    }

    /// Return number of votes received if candidate, or None if no longer Candidate.
    pub(crate) fn add_vote_if_candidate(&mut self, vote_from: ReplicaId) -> Option<usize> {
        if let State::Candidate(cs) = &mut self.state {
            Some(cs.add_received_vote(vote_from))
        } else {
            None
        }
    }

    pub(crate) fn leader_state_mut(&mut self) -> Option<&mut LeaderStateTracker> {
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
                leader: Some(leader_info),
                ..
            }) => write!(f, "Follower(Leader={:?})", leader_info.replica_id),
            State::Follower(FollowerState { leader: None, .. }) => write!(f, "Follower(Leader=None)"),
        }
    }
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
    leader: Option<LeaderRedirectInfo>,
    follower_timeout_tracker: FollowerTimerHandle,
}

impl LeaderState {
    fn new(
        peer_ids: HashSet<ReplicaId>,
        previous_log_entry_index: Option<Index>,
        heartbeat_duration: Duration,
        actor_client: WeakActorClient,
        term: Term,
    ) -> Self {
        let mut peer_state = HashMap::with_capacity(peer_ids.len());
        for peer_id in peer_ids {
            // TODO:3 eagerly broadcast AE from this task for the initial round.
            let leader_timer_handle =
                LeaderTimerHandle::spawn_timer_task(heartbeat_duration, actor_client.clone(), peer_id.clone(), term);
            peer_state.insert(peer_id, PeerState::new(leader_timer_handle, previous_log_entry_index));
        }

        Self {
            tracker: LeaderStateTracker::new(peer_state),
        }
    }
}

impl CandidateState {
    fn new(min_timeout: Duration, max_timeout: Duration, actor_client: WeakActorClient) -> Self {
        Self {
            received_votes_from: HashSet::with_capacity(3),
            _follower_timeout_tracker: FollowerTimerHandle::spawn_timer_task(min_timeout, max_timeout, actor_client),
        }
    }

    /// `add_received_vote()` returns the number of unique votes we've received after adding the
    /// provided `vote_from`
    fn add_received_vote(&mut self, vote_from: ReplicaId) -> usize {
        self.received_votes_from.insert(vote_from);
        self.received_votes_from.len()
    }
}

impl FollowerState {
    fn new(min_timeout: Duration, max_timeout: Duration, actor_client: WeakActorClient) -> Self {
        Self::with_leader_info(None, min_timeout, max_timeout, actor_client)
    }

    fn with_leader_info(
        leader: Option<LeaderRedirectInfo>,
        min_timeout: Duration,
        max_timeout: Duration,
        actor_client: WeakActorClient,
    ) -> Self {
        Self {
            leader,
            follower_timeout_tracker: FollowerTimerHandle::spawn_timer_task(min_timeout, max_timeout, actor_client),
        }
    }

    fn reset_timeout(&self) {
        self.follower_timeout_tracker.reset_timeout();
    }
}
