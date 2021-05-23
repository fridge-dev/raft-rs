use crate::actor::{ActorClient, ReplicaActor};
use crate::api::options::RaftOptionsValidated;
use crate::api::{commit_stream, MemberInfoBlob};
use crate::commitlog::InMemoryLog;
use crate::replica::{ClusterTracker, Replica, ReplicaConfig, VolatileLocalState};
use crate::server;
use crate::server::RpcServer;
use crate::{api, replica, CommitStream, EventListener, RaftOptions, ReplicatedLog};
use std::convert::TryFrom;
use std::error::Error;
use std::io;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

/// RaftClient is the conglomeration of all of the client facing components.
pub struct RaftClient {
    pub replication_log: ReplicatedLog,
    pub commit_stream: CommitStream,
    pub event_listener: EventListener,
}

impl RaftClient {
    pub fn destruct(self) -> (ReplicatedLog, CommitStream, EventListener) {
        (self.replication_log, self.commit_stream, self.event_listener)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ClientCreationError {
    #[error("Invalid cluster info")]
    InvalidClusterInfo(Box<dyn Error>),
    #[error("Illegal options for configuring client: {0}")]
    IllegalClientOptions(String),
    #[error("Log initialization failure")]
    LogInitialization(io::Error),
    // We will need to relax this later when adding membership changes.
    #[error("my replica ID not in cluster config")]
    MeNotInCluster,
}

pub async fn create_raft_client(config: RaftClientConfig) -> Result<RaftClient, ClientCreationError> {
    let root_logger = config.info_logger;

    let commit_log = InMemoryLog::create(root_logger.clone()).map_err(|e| ClientCreationError::LogInitialization(e))?;

    let cluster_tracker = try_create_cluster_tracker(root_logger.clone(), config.cluster_info.clone()).await?;
    let local_state = VolatileLocalState::new(cluster_tracker.my_replica_id().clone());

    let (commit_stream_publisher, commit_stream) = commit_stream::create_commit_stream();
    let (actor_client, actor_queue_rx) = ActorClient::new(10);

    let options = RaftOptionsValidated::try_from(config.options)?;

    let (server_shutdown_handle, server_shutdown_signal) = server::shutdown_signal();

    let (replica, election_state_change_listener) = Replica::new(ReplicaConfig {
        logger: root_logger.clone(),
        cluster_tracker,
        commit_log,
        local_state,
        commit_stream_publisher,
        server_shutdown_handle,
        actor_client: actor_client.weak(),
        leader_heartbeat_duration: options.leader_heartbeat_duration,
        follower_min_timeout: options.follower_min_timeout,
        follower_max_timeout: options.follower_max_timeout,
        append_entries_timeout: options.leader_append_entries_timeout,
    });

    let replica_actor = ReplicaActor::new(root_logger.clone(), actor_queue_rx, replica);
    tokio::spawn(replica_actor.run_event_loop());

    let server_addr = get_my_server_addr(&config.cluster_info)?;
    let replica_raft_server = RpcServer::new(root_logger.clone(), actor_client.weak());
    tokio::spawn(replica_raft_server.run(server_addr, server_shutdown_signal));

    let replication_log = ReplicatedLog::new(actor_client);

    let event_listener = EventListener::new(election_state_change_listener);

    Ok(RaftClient {
        replication_log,
        commit_stream,
        event_listener,
    })
}

pub struct RaftClientConfig {
    // A directory where we can create files and sub-directories to support the commit log.
    pub commit_log_directory: String, // TODO:3 use `Path`
    pub info_logger: slog::Logger,
    pub cluster_info: ClusterInfo,
    pub options: RaftOptions,
}

#[derive(Clone)]
pub struct ClusterInfo {
    pub my_replica_id: String,
    pub cluster_members: Vec<MemberInfo>,
}

#[derive(Clone)]
pub struct MemberInfo {
    pub replica_id: String,
    pub ip_addr: Ipv4Addr,
    pub raft_rpc_port: u16,
    pub peer_redirect_info_blob: MemberInfoBlob,
}

impl From<api::MemberInfo> for replica::ReplicaMetadata {
    fn from(member_info: api::MemberInfo) -> Self {
        replica::ReplicaMetadata::new(
            replica::ReplicaId::new(member_info.replica_id),
            member_info.ip_addr,
            member_info.raft_rpc_port,
            replica::ReplicaBlob::new(member_info.peer_redirect_info_blob.into_inner()),
        )
    }
}

async fn try_create_cluster_tracker(
    logger: slog::Logger,
    cluster_info: api::ClusterInfo,
) -> Result<replica::ClusterTracker, ClientCreationError> {
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

    ClusterTracker::create_valid_cluster(logger, my_md, peers_md)
        .await
        .map_err(|e| ClientCreationError::InvalidClusterInfo(e.into()))
}

fn get_my_server_addr(cluster_info: &api::ClusterInfo) -> Result<SocketAddr, ClientCreationError> {
    for member_info in cluster_info.cluster_members.iter() {
        if member_info.replica_id == cluster_info.my_replica_id {
            return Ok(SocketAddr::V4(SocketAddrV4::new(
                member_info.ip_addr,
                member_info.raft_rpc_port,
            )));
        }
    }

    Err(ClientCreationError::MeNotInCluster)
}
