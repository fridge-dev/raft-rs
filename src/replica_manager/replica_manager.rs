use crate::commitlog::{Entry, Log, LogFactory, LogConfig};
use crate::replica::{RaftReplica, RaftConfig};
use std::net::Ipv4Addr;
use std::collections::HashMap;

// A single node should be able to observe (and ultimately participate in)
// multiple clusters. A cluster is a logical construct that contains "replicas",
// not nodes. In other words, a replica participates in a cluster.
//
// On a single node, we will only participate at most one replica per cluster.
pub struct ReplicaManager<L: Log, F: LogFactory<L>> {
    me: Ipv4Addr,
    replica_by_cluster_id: HashMap<String, RaftReplica<L>>,
    log_factory: F,
}

pub struct ClusterConfig {
    pub cluster_members: Vec<Ipv4Addr>,
    pub cluster_id: String,
}

impl<L: Log, F: LogFactory<L>> ReplicaManager<L, F> {
    pub fn new(self_ip_addr: Ipv4Addr, log_factory: F) -> Self {
        ReplicaManager {
            me: self_ip_addr,
            replica_by_cluster_id: HashMap::new(),
            log_factory,
        }
    }

    pub fn observe_cluster(&mut self, config: ClusterConfig) {
        // First write wins
        if self.replica_by_cluster_id.contains_key(&config.cluster_id) {
            return
        }

        let replica = self.create_replica(config.cluster_id.clone(), config.cluster_members);
        self.replica_by_cluster_id.insert(config.cluster_id, replica);
    }

    fn create_replica(&self, cluster_id: String, cluster_members: Vec<Ipv4Addr>) -> RaftReplica<L> {
        let mut log = self.log_factory.try_create_log(LogConfig {
            cluster_id,
        }).expect("fail create log noo");

        // This is not part of raft lmao. Just testing out my method signatures.
        let result = log.append(Entry::new(vec![1, 2, 3, 4]));
        match result {
            Ok(index) => println!("Log initialized. Used index: {:?}", index),
            Err(e) => println!("Log initialization failed: {:?}", e)
        }

        RaftReplica::new(RaftConfig {
            me: self.me,
            cluster_members,
            log,
        })
    }

    pub fn replica(&self, cluster_id: &str) -> Option<&RaftReplica<L>> {
        self.replica_by_cluster_id.get(cluster_id)
    }
}
