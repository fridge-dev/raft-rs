use crate::commitlog::{Entry, Log, LogConfig, LogFactory};
use crate::replica::{
    MemberInfo, NoOpStateMachine, PersistentLocalState, RaftReplica, ReplicaConfig, ReplicaId,
    StateMachine, VolatileLocalState,
};
use std::collections::HashMap;

// A single node should be able to observe (and ultimately participate in)
// multiple clusters. A cluster is a logical construct that contains "replicas",
// not nodes. In other words, a replica participates in a cluster.
//
// On a single node, we will only participate at most one replica per cluster.
//
// TODO:2 This is overly complicated and probably unnecessary. Remove multi-replica/multi-cluster per node concept.
pub struct ReplicaManager<L: Log, F: LogFactory<L>, S: PersistentLocalState, M: StateMachine> {
    me: ReplicaId,
    replica_by_cluster_id: HashMap<String, RaftReplica<L, S, M>>,
    log_factory: F,
}

pub struct ClusterConfig {
    pub cluster_id: String,
    pub cluster_members: Vec<MemberInfo>,
}

impl<L: Log, F: LogFactory<L>, S: PersistentLocalState, M: StateMachine>
    ReplicaManager<L, F, S, M>
{
    pub fn new(my_id: ReplicaId, log_factory: F) -> Self {
        ReplicaManager {
            me: my_id,
            replica_by_cluster_id: HashMap::new(),
            log_factory,
        }
    }

    pub fn replica(&self, cluster_id: &str) -> Option<&RaftReplica<L, S, M>> {
        self.replica_by_cluster_id.get(cluster_id)
    }
}

impl<L: Log, F: LogFactory<L>> ReplicaManager<L, F, VolatileLocalState, NoOpStateMachine> {
    pub fn observe_cluster(&mut self, config: ClusterConfig) {
        // First write wins
        if self.replica_by_cluster_id.contains_key(&config.cluster_id) {
            return;
        }

        let replica = self.create_replica(config.cluster_id.clone(), config.cluster_members);
        self.replica_by_cluster_id
            .insert(config.cluster_id, replica);
    }

    fn create_replica(
        &self,
        cluster_id: String,
        cluster_members: Vec<MemberInfo>,
    ) -> RaftReplica<L, VolatileLocalState, NoOpStateMachine> {
        let mut log = self
            .log_factory
            .try_create_log(LogConfig { cluster_id })
            .expect("fail create log noo");

        // This is not part of raft lmao. Just testing out my method signatures.
        let result = log.append(Entry::new(vec![1, 2, 3, 4]));
        match result {
            Ok(index) => println!("Log initialized. Used index: {:?}", index),
            Err(e) => println!("Log initialization failed: {:?}", e),
        }

        RaftReplica::new(ReplicaConfig {
            me: self.me.clone(),
            cluster_members,
            log,
            local_state: VolatileLocalState::new(),
            state_machine: NoOpStateMachine::new(),
        })
    }
}
