use std::collections::HashMap;
use std::hash::Hash;
use std::net::Ipv4Addr;

/// ReplicaId is kind of like NodeId or ServerId. It is the ID of the entity participating in the
/// replication cluster.
#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub struct ReplicaId(String);

impl ReplicaId {
    pub fn new(replica_id: String) -> Self {
        ReplicaId(replica_id)
    }

    pub fn into_inner(self) -> String {
        self.0
    }
}

#[derive(Clone)]
pub struct ClusterMember {
    id: ReplicaId,
    ip: Ipv4Addr,
}

impl ClusterMember {
    pub fn new(replica_id: String, replica_ip: Ipv4Addr) -> Self {
        ClusterMember {
            id: ReplicaId(replica_id),
            ip: replica_ip,
        }
    }

    pub fn ip_addr(&self) -> Ipv4Addr {
        self.ip
    }
}

pub struct Cluster {
    my_replica_id: ReplicaId,
    cluster_members: HashMap<ReplicaId, ClusterMember>,
}

impl Cluster {
    /// Ensures that any instance of Cluster has expected properties upheld.
    pub fn create_valid_cluster(
        cluster_members: Vec<ClusterMember>,
        my_replica_id: String,
    ) -> Result<Self, InvalidCluster> {
        let cluster_members_by_id = map_with_unique_index(cluster_members, |m| m.id.clone())
            .map_err(|dupe| InvalidCluster::DuplicateReplicaId(dupe.0))?;

        let my_replica_id_typed = ReplicaId(my_replica_id);

        // We might need to relax this later when adding membership changes.
        if !cluster_members_by_id.contains_key(&my_replica_id_typed) {
            return Err(InvalidCluster::MeNotInCluster);
        }

        Ok(Cluster {
            my_replica_id: my_replica_id_typed,
            cluster_members: cluster_members_by_id,
        })
    }

    pub(super) fn destruct(self) -> (ReplicaId, HashMap<ReplicaId, ClusterMember>) {
        (self.my_replica_id, self.cluster_members)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum InvalidCluster {
    #[error("duplicate replica '{0}' in cluster config")]
    DuplicateReplicaId(String),
    #[error("my replica ID not in cluster config")]
    MeNotInCluster,
}

/// Returns a HashMap that is guaranteed to have uniquely indexed all of the values. If duplicate is
/// present, the key for the duplicate is returned as an Err.
fn map_with_unique_index<K, V, F>(values: Vec<V>, key_for_value: F) -> Result<HashMap<K, V>, K>
where
    K: Hash + Eq,
    F: Fn(&V) -> K,
{
    let mut map = HashMap::with_capacity(values.len());

    for v in values {
        if let Some(duplicate) = map.insert(key_for_value(&v), v) {
            return Err(key_for_value(&duplicate));
        }
    }

    Ok(map)
}
