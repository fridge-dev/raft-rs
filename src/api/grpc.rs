use crate::commitlog::{Log, LogFactory};
use crate::replica::{
    MemberInfo, NoOpStateMachine, PersistentLocalState, ReplicaId, StateMachine, VolatileLocalState,
};
use crate::{ClusterConfig, ReplicaManager};
use std::net::Ipv4Addr;

pub struct GrpcServer<L, F, S, M>
where
    L: Log,
    F: LogFactory<L>,
    S: PersistentLocalState,
    M: StateMachine,
{
    replica_manager: ReplicaManager<L, F, S, M>,
    // Single gRPC server with these RPCs:

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
}

impl<L, F, S, M> GrpcServer<L, F, S, M>
where
    L: Log,
    F: LogFactory<L>,
    S: PersistentLocalState,
    M: StateMachine,
{
    pub fn new(replica_manager: ReplicaManager<L, F, S, M>) -> Self {
        GrpcServer { replica_manager }
    }
}

impl<L, F> GrpcServer<L, F, VolatileLocalState, NoOpStateMachine>
where
    L: Log,
    F: LogFactory<L>,
{
    pub fn run(mut self) {
        self.replica_manager.observe_cluster(ClusterConfig {
            cluster_id: "helloworld".to_string(),
            cluster_members: vec![
                MemberInfo {
                    id: ReplicaId("id-1".into()),
                    ip: Ipv4Addr::from(0xFACE),
                },
                MemberInfo {
                    id: ReplicaId("id-2".into()),
                    ip: Ipv4Addr::from(0xBEEF),
                },
                MemberInfo {
                    id: ReplicaId("id-3".into()),
                    ip: Ipv4Addr::from(0x1337),
                },
                MemberInfo {
                    id: ReplicaId("id-4".into()),
                    ip: Ipv4Addr::from(0xDEAF),
                },
                MemberInfo {
                    id: ReplicaId("id-5".into()),
                    ip: Ipv4Addr::from(0xBEEB),
                },
            ],
        })
    }
}
