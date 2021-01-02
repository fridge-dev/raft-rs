use crate::commitlog::{Log, Index};
use std::net::Ipv4Addr;

pub struct RaftConfig<L: Log> {
    pub me: Ipv4Addr,
    pub cluster_members: Vec<Ipv4Addr>,
    pub log: L,
}

// We need this internally.
pub trait RaftRpcHandler {
    fn handle_request_vote();
    fn handle_append_entries();
}

pub trait RaftClient {
    // Will attempt to discover leader to trigger a replication of data
    fn send_request(/* app specific types?, no, same as append_entries() */);
}

pub struct RaftReplica<L: Log> {
    me: Ipv4Addr,
    cluster_members: Vec<Ipv4Addr>,
    log: L,
    term: u64,
    latest_index: Index,
    state: ElectionState,
}

impl<L: Log> RaftReplica<L> {
    pub fn new(config: RaftConfig<L>) -> Self {
        let latest_index = config.log.next_index();
        RaftReplica {
            me: config.me,
            cluster_members: config.cluster_members,
            log: config.log,
            term: 0,
            latest_index,
            state: ElectionState::Follower(FollowerState{}),
        }
    }
}

///////////////

enum ElectionState {
    Leader(LeaderState),
    Candidate(CandidateState),
    Follower(FollowerState),
}

impl ElectionState {
    // for proof of concept
    pub fn cycle(self) -> Self {
        match self {
            ElectionState::Leader(l) => ElectionState::Follower(l.into()),
            ElectionState::Candidate(c) => ElectionState::Leader(c.into()),
            ElectionState::Follower(f) => ElectionState::Candidate(f.into()),
        }
    }
}

struct LeaderState {

}

impl From<CandidateState> for LeaderState {
    fn from(_: CandidateState) -> Self {
        LeaderState {}
    }
}

impl From<CandidateState> for FollowerState {
    fn from(_: CandidateState) -> Self {
        FollowerState {}
    }
}

struct CandidateState {

}

impl From<FollowerState> for CandidateState {
    fn from(_: FollowerState) -> Self {
        CandidateState {}
    }
}

struct FollowerState {

}

impl From<LeaderState> for FollowerState {
    fn from(_: LeaderState) -> Self {
        FollowerState {}
    }
}