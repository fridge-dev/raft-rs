use crate::commitlog::Index;
use crate::ReplicaId;
use std::collections::HashMap;

pub struct ElectionState {
    // Invariant: `state: Option<>` is always `Some` at beginning and end of method.
    state: Option<State>,
}

impl ElectionState {
    /// `new_follower()` creates a new ElectionState instance that starts out as a follower.
    pub fn new_follower() -> Self {
        ElectionState {
            state: Some(State::Follower(FollowerState {})),
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

pub struct LeaderState {
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
    pub fn new(/* peers should be listed here*/) -> Self {
        LeaderState {
            peer_state: HashMap::new(),
        }
    }

    pub fn into_follower(self) -> FollowerState {
        FollowerState {}
    }
}

pub struct CandidateState {}

impl CandidateState {
    pub fn into_leader(self) -> LeaderState {
        LeaderState::new()
    }

    pub fn into_follower(self) -> FollowerState {
        FollowerState {}
    }

    // into_candidate? split vote?
}

pub struct FollowerState {}

impl FollowerState {
    pub fn into_candidate(self) -> CandidateState {
        CandidateState {}
    }
}
