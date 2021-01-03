use crate::commitlog::{Log, Index};
use crate::replica::election::{ElectionState, FollowerState};
use crate::replica::state_machine::StateMachine;
use crate::replica::local_state::PersistentLocalState;
use crate::replica::peers::MemberInfo;
use crate::replica::ReplicaId;

pub type Term = u64;

pub struct RaftReplica<L: Log, S: PersistentLocalState, M: StateMachine> {
    me: ReplicaId,
    cluster_members: Vec<MemberInfo>,
    local_state: S,
    election_state: ElectionState,

    // This is the log that we're replicating.
    log: L,
    // Index of highest log entry that we've locally written.
    latest_index: Index, // TODO: is this needed??
    // Index of highest log entry known to be committed.
    commit_index: Index,
    // Index of highest log entry applied to state machine.
    last_applied_index: Index,
    // User provided state machine for applying log entries.
    state_machine: M,
}

pub struct ReplicaConfig<L: Log, S: PersistentLocalState, M: StateMachine> {
    pub me: ReplicaId,
    pub cluster_members: Vec<MemberInfo>,
    pub log: L,
    pub local_state: S,
    pub state_machine: M,
}

impl<L: Log, S: PersistentLocalState, M: StateMachine> RaftReplica<L, S, M> {
    pub fn new(config: ReplicaConfig<L, S, M>) -> Self {
        let latest_index = config.log.next_index();
        RaftReplica {
            me: config.me,
            cluster_members: config.cluster_members,
            local_state: config.local_state,
            election_state: ElectionState::Follower(FollowerState{}),
            log: config.log,
            latest_index,
            commit_index: Index::new(0),
            last_applied_index: Index::new(0),
            state_machine: config.state_machine,
        }
    }
}