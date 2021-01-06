use crate::commitlog::{InMemoryLog, InMemoryLogFactory, Log, LogConfig, LogFactory};
use crate::replica::{
    PersistentLocalState, RaftLogEntry, RaftReplica, ReplicaConfig, ReplicaId, StateMachine,
    VolatileLocalState,
};
use crate::ClusterConfig;

// Single gRPC server with these RPCs:
//
// Raft data plane - this is the primitive building block.
// - AppendEntries({cluster_id})
// - RequestVote({cluster_id})
//
// Raft control plane - used during raft deployment/cycling (infrequent). They ultimately delegate towards raft data plane.
// - ObserveCluster({cluster_id}) - for adding self to cluster
// - LeaveCluster({cluster_id}) - for removing self from cluster
// - UpdateCluster({cluster_id}) - for existing members to become aware of added/removed members
//
// Public facing API: KV store - supports redirects. Ultimately results in raft data plane calls.
// Put({cluster_id}, key, value)
// Get({cluster_id}, key)
// Delete({cluster_id}, key)
pub struct GrpcServer<L, S, M>
where
    L: Log<RaftLogEntry>,
    S: PersistentLocalState,
    M: StateMachine,
{
    replica: RaftReplica<L, S, M>,
}

impl<M> GrpcServer<InMemoryLog<RaftLogEntry>, VolatileLocalState, M>
where
    M: StateMachine,
{
    pub fn new_local(
        cluster_config: ClusterConfig,
        my_replica_id: ReplicaId,
        state_machine: M,
    ) -> Self {
        let log = InMemoryLogFactory::new()
            .try_create_log(LogConfig {
                cluster_id: cluster_config.cluster_id,
            })
            .expect("that shit can't fail");

        let replica = RaftReplica::new(ReplicaConfig {
            my_replica_id,
            cluster_members: cluster_config.cluster_members,
            log,
            local_state: VolatileLocalState::new(),
            state_machine,
        });

        GrpcServer { replica }
    }

    pub fn run(self) {
        panic!("aaahhh!!!")
    }
}
