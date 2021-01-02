use crate::commitlog::Index;
use std::collections::HashMap;

pub enum ElectionState {
    Leader(LeaderState),
    Candidate(CandidateState),
    Follower(FollowerState),
}

impl ElectionState {
    // for proof of concept
    pub fn cycle(self) -> Self {
        match self {
            ElectionState::Follower(follower) => ElectionState::Candidate(follower.into_candidate()),
            ElectionState::Candidate(candidate) => ElectionState::Leader(candidate.into_leader()),
            ElectionState::Leader(leader) => ElectionState::Follower(leader.into_follower()),
        }
    }
}

pub struct LeaderState {
    peer_state: HashMap<String, LeaderServerView>,
}

struct LeaderServerView {
    next: Index,
    matched: Index,
}

impl LeaderServerView {
    pub fn new() -> Self {
        LeaderServerView {
            next: Index(0),
            matched: Index(0),
        }
    }
}

impl LeaderState {
    pub fn new(/* peers should be listed here*/) -> Self {
        LeaderState {
            peer_state: HashMap::new()
        }
    }

    pub fn into_follower(self) -> FollowerState {
        FollowerState {}
    }
}

pub struct CandidateState {

}

impl CandidateState {
    pub fn into_leader(self) -> LeaderState {
        LeaderState::new()
    }

    pub fn into_follower(self) -> FollowerState {
        FollowerState {}
    }

    // into_candidate? split vote?
}

pub struct FollowerState {

}

impl FollowerState {
    pub fn into_candidate(self) -> CandidateState {
        CandidateState {}
    }
}
