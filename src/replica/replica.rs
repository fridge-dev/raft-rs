use crate::commitlog::{Log, Index};
use std::net::Ipv4Addr;
use crate::replica::election::{ElectionState, FollowerState};
use crate::replica::state_machine::StateMachine;
use crate::replica::local_state::PersistentLocalState;

pub struct RaftReplica<L: Log> {
    me: Ipv4Addr,
    cluster_members: Vec<Ipv4Addr>,
    local_state: PersistentLocalState,
    election_state: ElectionState,

    // This is the log that we're replicating.
    log: L,
    // Index of highest log entry that we've locally written.
    latest_index: Index, // TODO: is this needed??
    // Index of highest log entry known to be committed.
    commit_index: Index,
    // Index of highest log entry applied to state machine.
    last_applied_index: Index,

    state_machine: StateMachine,
}

pub struct ReplicaConfig<L: Log> {
    pub me: Ipv4Addr,
    pub cluster_members: Vec<Ipv4Addr>,
    pub log: L,
}

impl<L: Log> RaftReplica<L> {
    pub fn new(config: ReplicaConfig<L>) -> Self {
        let latest_index = config.log.next_index();
        RaftReplica {
            me: config.me,
            cluster_members: config.cluster_members,
            local_state: PersistentLocalState::new(),
            election_state: ElectionState::Follower(FollowerState{}),
            log: config.log,
            latest_index,
            commit_index: Index(0),
            last_applied_index: Index(0),
            state_machine: StateMachine {}
        }
    }
}