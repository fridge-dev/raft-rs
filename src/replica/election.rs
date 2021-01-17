use crate::commitlog::Index;
use crate::replica::peers::ReplicaId;
use std::collections::HashMap;

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
            State::Follower(FollowerState { leader_id: None, .. }) => CurrentLeader::Unknown,
            State::Follower(FollowerState {
                leader_id: Some(leader_id),
                ..
            }) => CurrentLeader::Other(leader_id.clone()),
        }
    }

    pub fn transition_to_follower(&mut self, new_leader_id: Option<ReplicaId>) {
        self.do_transition(|state| state.into_follower(new_leader_id));
    }

    fn do_transition<F>(&mut self, transition: F)
    where
        F: FnOnce(State) -> State,
    {
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
    pub fn into_follower(self, new_leader_id: Option<ReplicaId>) -> Self {
        match self {
            // TODO:3 consider making a trait like below as an experiment in readability.
            //        `IntoFollower { fn into_follower(self, ...) -> FollowerState }`
            //        Or consider changing `ElectionState.transition_to_follower` to directly
            //        create follower state :P
            State::Leader(leader) => State::Follower(leader.into_follower(new_leader_id)),
            State::Candidate(candidate) => State::Follower(candidate.into_follower(new_leader_id)),
            State::Follower(follower) => State::Follower(follower.into_follower(new_leader_id)),
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
    pub fn new(/* TODO:1.5 peers should be listed here*/) -> Self {
        LeaderState {
            peer_state: HashMap::new(),
        }
    }

    pub fn into_follower(self, new_leader_id: Option<ReplicaId>) -> FollowerState {
        FollowerState::with_leader_info(new_leader_id)
    }
}

struct CandidateState {}

impl CandidateState {
    pub fn into_leader(self) -> LeaderState {
        LeaderState::new()
    }

    pub fn into_follower(self, new_leader_id: Option<ReplicaId>) -> FollowerState {
        FollowerState::with_leader_info(new_leader_id)
    }

    // into_candidate? split vote?
}

struct FollowerState {
    leader_id: Option<ReplicaId>,
}

impl FollowerState {
    pub fn new() -> Self {
        FollowerState { leader_id: None }
    }

    pub fn with_leader_info(leader_id: Option<ReplicaId>) -> Self {
        FollowerState { leader_id }
    }

    pub fn into_follower(self, new_leader_id: Option<ReplicaId>) -> FollowerState {
        Self::with_leader_info(new_leader_id)
    }

    pub fn into_candidate(self) -> CandidateState {
        CandidateState {}
    }
}
