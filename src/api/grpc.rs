use crate::{ReplicaManager, ClusterConfig};
use crate::commitlog::{Log, LogFactory};
use std::net::Ipv4Addr;

pub struct GrpcServer<L, F> where L: Log, F: LogFactory<L> {
    replica_manager: ReplicaManager<L, F>,


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

impl<L, F> GrpcServer<L, F> where L: Log, F: LogFactory<L> {
    pub fn new(replica_manager: ReplicaManager<L, F>) -> Self {
        GrpcServer {
            replica_manager
        }
    }

    pub fn run(mut self) {
        self.replica_manager.observe_cluster(ClusterConfig {
            cluster_members: vec![
                Ipv4Addr::from(0xFACE),
                Ipv4Addr::from(0xBEEF),
                Ipv4Addr::from(0x1337),
                Ipv4Addr::from(0xDEAF),
                Ipv4Addr::from(0xBEEB),
            ],
            cluster_id: "helloworld".to_string(),
        })
    }
}