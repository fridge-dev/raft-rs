use crate::actor::{ActorClient, ReplicaActor};
use crate::api::client;
use crate::api::client::{ClientAdapter, CommitStream};
use crate::commitlog::InMemoryLog;
use crate::replica::{PeerTracker, Replica, ReplicaConfig, VolatileLocalState};
use crate::server::ServerAdapter;
use crate::{api, replica, RaftClientConfig, ReplicatedLog};
use std::error::Error;
use std::io;
use std::net::{SocketAddr, SocketAddrV4};
use tokio::sync::mpsc;

pub async fn create_raft_client(config: RaftClientConfig) -> Result<CreatedClient, ClientCreationError> {
    let log = InMemoryLog::create().map_err(|e| ClientCreationError::LogInitialization(e))?;

    let peer_tracker = try_create_cluster(config.cluster_info.clone()).await?;
    let local_state = VolatileLocalState::new(peer_tracker.my_replica_id().clone());

    let (commit_stream_publisher, commit_stream) = client::create_commit_stream();
    let (actor_queue_tx, actor_queue_rx) = mpsc::channel(10);
    let actor_client = ActorClient::new(actor_queue_tx);

    let replica = Replica::new(ReplicaConfig {
        peer_tracker,
        log,
        local_state,
        commit_stream_publisher,
        actor_client: actor_client.clone(),
        leader_heartbeat_duration: config.leader_heartbeat_duration,
        follower_min_timeout: config.follower_min_timeout,
        follower_max_timeout: config.follower_max_timeout,
    });

    let replica_actor = ReplicaActor::new(actor_queue_rx, replica);
    tokio::spawn(replica_actor.run_event_loop());

    let server_addr = get_my_server_addr(&config.cluster_info)?;
    let replica_raft_server = ServerAdapter::new(config.cluster_info.my_replica_id, actor_client.clone());
    tokio::spawn(replica_raft_server.run(server_addr));

    let client = ClientAdapter { actor_client };

    Ok(CreatedClient {
        replication_log: Box::new(client),
        commit_stream,
    })
}

// Name could be better
pub struct CreatedClient {
    pub replication_log: Box<dyn ReplicatedLog>,
    pub commit_stream: CommitStream,
}

#[derive(Debug, thiserror::Error)]
pub enum ClientCreationError {
    #[error("Invalid cluster info")]
    InvalidClusterInfo(Box<dyn Error>),
    #[error("Log initialization failure")]
    LogInitialization(io::Error),
    // We will need to relax this later when adding membership changes.
    #[error("my replica ID not in cluster config")]
    MeNotInCluster,
}

fn get_my_server_addr(cluster_info: &api::ClusterInfo) -> Result<SocketAddr, ClientCreationError> {
    for member_info in cluster_info.cluster_members.iter() {
        if member_info.replica_id == cluster_info.my_replica_id {
            return Ok(SocketAddr::V4(SocketAddrV4::new(
                member_info.replica_ip_addr,
                member_info.replica_port,
            )));
        }
    }

    Err(ClientCreationError::MeNotInCluster)
}

async fn try_create_cluster(cluster_info: api::ClusterInfo) -> Result<replica::PeerTracker, ClientCreationError> {
    let mut my_md = None;
    let mut peers_md = Vec::with_capacity(cluster_info.cluster_members.len() - 1);
    for member_info in cluster_info.cluster_members.into_iter() {
        if member_info.replica_id == cluster_info.my_replica_id {
            my_md = Some(member_info.into());
        } else {
            peers_md.push(member_info.into());
        }
    }

    let my_md = my_md.ok_or_else(|| ClientCreationError::MeNotInCluster)?;

    PeerTracker::create_valid_cluster(my_md, peers_md)
        .await
        .map_err(|e| ClientCreationError::InvalidClusterInfo(e.into()))
}

impl From<api::MemberInfo> for replica::ReplicaMetadata {
    fn from(member_info: api::MemberInfo) -> Self {
        replica::ReplicaMetadata::new(
            member_info.replica_id,
            member_info.replica_ip_addr,
            member_info.replica_port,
        )
    }
}
