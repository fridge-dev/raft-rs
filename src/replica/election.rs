use crate::actor;
use crate::commitlog::Index;
use crate::replica::peers::ReplicaId;
use crate::replica::timers::{FollowerTimerHandle, LeaderTimerHandle};
use crate::replica::Term;
use std::collections::{HashMap, HashSet};
use std::fmt;
use tokio::time::Duration;

#[derive(Copy, Clone)]
pub struct ElectionConfig {
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

    /// `add_received_vote()` returns the number of unique votes we've received after adding the
    /// provided `vote_from`
    pub fn add_received_vote_if_candidate(&mut self, term: Term, vote_from: ReplicaId) -> usize {
        if let State::Candidate(cs) = &mut self.state {
            if cs.term == term {
                cs.add_received_vote(vote_from)
            } else {
                // Received response for outdated term.
                0
            }
        } else {
            0
        }
    }

    // TODO:2 possible name: `on_follower_timeout()`
    pub fn transition_to_candidate(&mut self, term: Term) {
        self.state = State::Candidate(CandidateState::new(
            term,
            self.config.follower_min_timeout,
            self.config.follower_max_timeout,
            self.actor_client.clone(),
        ));
    }

    pub fn is_election_open(&self, term: Term) -> bool {
        if let State::Candidate(cs) = &self.state {
            cs.term == term
        } else {
            false
        }
    }

    // TODO:2 possible name: `on_majority_votes`
    pub fn transition_to_leader_if_not(&mut self) {
        // TODO:1 figure out how to model validate state machines better.
        //        Maybe add Term and have a FSM with guaranteed termination
        //        that resets every Term incr.
        if let State::Leader(_) = &self.state {
            return;
        }

        self.state = State::Leader(LeaderState::new(
            self.config.leader_heartbeat_duration,
            self.actor_client.clone(),
        ))
    }

    pub fn transition_to_follower(&mut self, new_leader_id: Option<ReplicaId>) {
        self.state = State::Follower(FollowerState::with_leader_info(
            new_leader_id,
            self.config.follower_min_timeout,
            self.config.follower_max_timeout,
            self.actor_client.clone(),
        ));
    }
}

impl fmt::Debug for ElectionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.state {
            State::Leader(_) => write!(f, "Leader"),
            State::Candidate(cs) => write!(f, "Candidate(Term={})", cs.term),
            State::Follower(FollowerState {
                leader_id: Some(leader_id),
                ..
            }) => write!(f, "Follower(Leader={})", leader_id),
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
    peer_state: HashMap<ReplicaId, LeaderServerView>,
    heartbeat_timer: LeaderTimerHandle,
}

struct CandidateState {
    term: Term,
    received_votes_from: HashSet<ReplicaId>,
    follower_timeout_tracker: FollowerTimerHandle,
}

struct FollowerState {
    leader_id: Option<ReplicaId>,
    follower_timeout_tracker: FollowerTimerHandle,
}

impl LeaderState {
    pub fn new(
        /* TODO:1.5 peers should be listed here */
        heartbeat_duration: Duration,
        actor_client: actor::ActorClient,
    ) -> Self {
        LeaderState {
            peer_state: HashMap::new(),
            heartbeat_timer: LeaderTimerHandle::spawn_background_task(heartbeat_duration, actor_client),
        }
    }
}

impl CandidateState {
    pub fn new(term: Term, min_timeout: Duration, max_timeout: Duration, actor_client: actor::ActorClient) -> Self {
        CandidateState {
            term,
            // with_capacity(3)? CmonBruh, you can't assume that.
            // Well... Because of jittered follower timeout, most of the time if a follower
            // transitions to candidate, they'll also transition to leader. And most of the time, I
            // will run this as a cluster of 5, so we only need 3. If we run cluster at different
            // size, this won't matter hardly at all.
            received_votes_from: HashSet::with_capacity(3),
            follower_timeout_tracker: FollowerTimerHandle::spawn_background_task(
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

// Consider separate mod for leader state tracking, if it's more involved.
struct LeaderServerView {
    next: Index,
    matched: Index,
}

impl LeaderServerView {
    pub fn new() -> Self {
        LeaderServerView {
            next: Index::new(0),
            matched: Index::new(0),
        }
    }
}
