use crate::replica::peer_client::PeerRpcClient;
use std::collections::hash_map::Values;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::hash::Hash;
use std::net::Ipv4Addr;
use tonic::codegen::http::uri;

/// ReplicaId is kind of like NodeId or ServerId. It is the ID of the entity participating in the
/// replication cluster.
#[derive(Clone, Hash, Eq, PartialEq)]
pub(crate) struct ReplicaId(String);

impl ReplicaId {
    pub(crate) fn new(replica_id: impl Into<String>) -> Self {
        ReplicaId(replica_id.into())
    }

    pub(crate) fn into_inner(self) -> String {
        self.0
    }
}

impl fmt::Debug for ReplicaId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Copy, Clone, Debug)]
pub(crate) struct ReplicaInfoBlob(u128);

impl ReplicaInfoBlob {
    pub(crate) fn new(blob: u128) -> Self {
        ReplicaInfoBlob(blob)
    }

    pub(crate) fn into_inner(self) -> u128 {
        self.0
    }
}

/// ReplicaMetadata is identity/connection metadata describing a replica.
#[derive(Clone)]
pub(crate) struct ReplicaMetadata {
    id: ReplicaId,
    ip: Ipv4Addr,
    port: u16,
    blob: ReplicaInfoBlob,
}

impl ReplicaMetadata {
    pub(crate) fn new(replica_id: ReplicaId, ip_addr: Ipv4Addr, port: u16, blob: ReplicaInfoBlob) -> Self {
        ReplicaMetadata {
            id: replica_id,
            ip: ip_addr,
            port,
            blob,
        }
    }

    pub(super) fn replica_id(&self) -> &ReplicaId {
        &self.id
    }

    pub(super) fn ip_addr(&self) -> Ipv4Addr {
        self.ip
    }

    pub(super) fn info_blob(&self) -> ReplicaInfoBlob {
        self.blob
    }
}

/// Peer is a replica that is not me.
#[derive(Clone)]
pub(super) struct Peer {
    pub(super) metadata: ReplicaMetadata,
    pub(super) client: PeerRpcClient,
}

/// ClusterTracker is the group of replicas participating in a single instance of raft together.
// TODO:1 refactor replica wiring and then change to `pub(super)`
pub(crate) struct ClusterTracker {
    my_replica_metadata: ReplicaMetadata,
    peers: HashMap<ReplicaId, Peer>,
}

// Associated factory functions
impl ClusterTracker {
    // TODO:1 refactor replica wiring and then change to `pub(super)`
    // TODO:1 make sync and use connect_lazy()
    pub(crate) async fn create_valid_cluster(
        logger: slog::Logger,
        my_replica_metadata: ReplicaMetadata,
        peer_replica_metadata: Vec<ReplicaMetadata>,
    ) -> Result<Self, InvalidCluster> {
        let cluster_members_by_id = map_with_unique_index(peer_replica_metadata, |m| m.id.clone())
            .map_err(|dupe| InvalidCluster::DuplicateReplicaId(dupe.into_inner()))?;

        if cluster_members_by_id.contains_key(&my_replica_metadata.id) {
            return Err(InvalidCluster::DuplicateReplicaId(my_replica_metadata.id.into_inner()));
        }

        let peers = ClusterTracker::create_peers(logger, cluster_members_by_id).await?;

        Ok(ClusterTracker {
            my_replica_metadata,
            peers,
        })
    }

    async fn create_peers(
        logger: slog::Logger,
        cluster_members_by_id: HashMap<ReplicaId, ReplicaMetadata>,
    ) -> Result<HashMap<ReplicaId, Peer>, InvalidCluster> {
        let mut peers: HashMap<ReplicaId, Peer> = HashMap::with_capacity(cluster_members_by_id.len());
        for (peer_replica_id, peer_md) in cluster_members_by_id.into_iter() {
            let peer = Self::make_peer(logger.clone(), peer_md).await?;
            peers.insert(peer_replica_id, peer);
        }
        Ok(peers)
    }

    async fn make_peer(logger: slog::Logger, peer_md: ReplicaMetadata) -> Result<Peer, InvalidCluster> {
        let peer_client_logger = logger.new(slog::o!(
            "RemoteReplicaId" => peer_md.id.clone().into_inner(),
            "RemoteIpAddr" => format!("{}:{}", peer_md.ip, peer_md.port),
        ));
        let uri = Self::make_uri(peer_md.ip, peer_md.port)?;
        let client = PeerRpcClient::new(peer_client_logger, uri).await;
        Ok(Peer {
            metadata: peer_md,
            client,
        })
    }

    fn make_uri(ip: Ipv4Addr, port: u16) -> Result<uri::Uri, uri::InvalidUri> {
        let ip_octets = ip.octets();
        let url = format!(
            "http://{}.{}.{}.{}:{}",
            ip_octets[0], ip_octets[1], ip_octets[2], ip_octets[3], port
        );
        uri::Uri::from_maybe_shared(url)
    }
}

// Methods
impl ClusterTracker {
    pub(super) fn my_replica_id(&self) -> &ReplicaId {
        &self.my_replica_metadata.id
    }

    pub(super) fn contains_member(&self, id: &ReplicaId) -> bool {
        self.peers.contains_key(id)
    }

    // Exposing HashMap type, but experimenting with this style.
    pub(super) fn iter_peers(&self) -> Values<'_, ReplicaId, Peer> {
        self.peers.values()
    }

    pub(super) fn peer_ids(&self) -> HashSet<ReplicaId> {
        self.peers.keys().cloned().collect()
    }

    pub(super) fn peer(&self, id: &ReplicaId) -> Option<&Peer> {
        self.peers.get(id)
    }

    pub(super) fn metadata(&self, id: &ReplicaId) -> Option<&ReplicaMetadata> {
        self.peers.get(id).map(|peer| &peer.metadata)
    }

    /// `num_voting_replicas` returns the total number of voting replicas (including self) to
    /// participate in elections.
    pub(super) fn num_voting_replicas(&self) -> usize {
        // Currently, we don't support non-voting peers, so we just count
        // peers + self.
        self.peers.len() + 1
    }
}

// TODO:1 refactor replica wiring and then change to `pub(super)`
#[derive(Debug, thiserror::Error)]
pub(crate) enum InvalidCluster {
    #[error("duplicate replica '{0}' in cluster config")]
    DuplicateReplicaId(String),
    #[error("invalid URI")]
    InvalidUri(#[from] uri::InvalidUri),
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
