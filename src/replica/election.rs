use crate::commitlog::Index;
use std::collections::HashMap;
use crate::replica::peers::ReplicaId;

pub struct ElectionState {
    // Invariant: `state: Option<>` is always `Some` at beginning and end of method.
    state: Option<State>,
}

impl ElectionState {
    /// `new_follower()` creates a new ElectionState instance that starts out as a follower.
    pub fn new_follower() -> Self {
        ElectionState {
            state: Some(State::Follower(FollowerState::new())),
        }
    }

    pub fn current_leader(&self) -> CurrentLeader {
        let state = self
            .state
            .as_ref()
            .expect("Illegal state: ElectionStateManager has None inner state.");
        match state {
            State::Leader(_) => CurrentLeader::Me,
            State::Candidate(_) => CurrentLeader::Unknown,
            State::Follower(FollowerState { leader: None, .. }) => CurrentLeader::Unknown,
            State::Follower(FollowerState {
                leader: Some(leader_id),
                ..
            }) => CurrentLeader::Other(leader_id.clone()),
        }
    }

    pub fn transition_to_follower(&mut self) {
        self.do_transition(|state| state.into_follower());
    }

    fn do_transition(&mut self, transition: fn(State) -> State) {
        let mut state = self
            .state
            .take()
            .expect("Illegal state: ElectionStateManager has None inner state.");

        state = transition(state);

        self.state.replace(state);
    }
}

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

impl State {
    pub fn into_follower(self) -> Self {
        match self {
            State::Leader(leader) => State::Follower(leader.into_follower()),
            State::Candidate(candidate) => State::Follower(candidate.into_follower()),
            State::Follower(follower) => State::Follower(follower),
        }
    }
}

struct LeaderState {
    peer_state: HashMap<ReplicaId, LeaderServerView>,
}

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

impl LeaderState {
    pub fn new(/* TODO:1 peers should be listed here*/) -> Self {
        LeaderState {
            peer_state: HashMap::new(),
        }
    }

    pub fn into_follower(self) -> FollowerState {
        FollowerState::new()
    }
}

struct CandidateState {}

impl CandidateState {
    pub fn into_leader(self) -> LeaderState {
        LeaderState::new()
    }

    pub fn into_follower(self) -> FollowerState {
        FollowerState::new()
    }

    // into_candidate? split vote?
}

struct FollowerState {
    leader: Option<ReplicaId>,
}

impl FollowerState {
    // TODO:1 transition to follower with known leader
    pub fn new() -> Self {
        FollowerState { leader: None }
    }

    pub fn into_candidate(self) -> CandidateState {
        CandidateState {}
    }
}
