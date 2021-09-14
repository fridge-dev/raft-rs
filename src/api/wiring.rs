use crate::actor::{ActorClient, ReplicaActor};
use crate::api::client::RaftClient;
use crate::api::commit_stream;
use crate::api::options::RaftOptionsValidated;
use crate::api::types::RaftMemberInfo;
use crate::commitlog::InMemoryLog;
use crate::replica::{ClusterTracker, Replica, ReplicaConfig, ReplicaId, ReplicaMetadata, VolatileLocalState};
use crate::server;
use crate::server::RpcServer;
use crate::{RaftEventListener, RaftOptions, ReplicatedLog};
use std::convert::TryFrom;
use std::error::Error;
use std::io;
use std::net::{SocketAddr, SocketAddrV4};

pub struct RaftClientConfig {
    pub my_replica_id: String,
    pub cluster_members: Vec<RaftMemberInfo>,
    // A directory where we can create files and sub-directories to support the commit log.
    pub commit_log_directory: String, // TODO:3 use `Path`
    pub info_logger: slog::Logger,
    pub options: RaftOptions,
}

#[derive(Debug, thiserror::Error)]
pub enum RaftClientCreationError {
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

pub async fn try_create_raft_client(mut config: RaftClientConfig) -> Result<RaftClient, RaftClientCreationError> {
    let root_logger = config.info_logger;

    let commit_log =
        InMemoryLog::create(root_logger.clone()).map_err(|e| RaftClientCreationError::LogInitialization(e))?;
    let local_state = VolatileLocalState::new(ReplicaId::new(&config.my_replica_id));

    let my_member_info = take_me_out(&config.my_replica_id, &mut config.cluster_members)
        .ok_or_else(|| RaftClientCreationError::MeNotInCluster)?;
    let my_server_addr = raft_rpc_server_addr(&my_member_info);

    let cluster_tracker =
        try_create_cluster_tracker(root_logger.clone(), my_member_info, config.cluster_members).await?;

    let (commit_stream_publisher, commit_stream) = commit_stream::new();
    let (actor_client, actor_queue_rx) = ActorClient::new(10);

    let options = RaftOptionsValidated::try_from(config.options)
        .map_err(|e| RaftClientCreationError::IllegalClientOptions(e.to_string()))?;

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

    let replica_raft_server = RpcServer::new(root_logger.clone(), actor_client.weak());
    tokio::spawn(replica_raft_server.run(my_server_addr, server_shutdown_signal));

    let replicated_log = ReplicatedLog::new(actor_client);

    let event_listener = RaftEventListener::new(election_state_change_listener);

    Ok(RaftClient {
        replicated_log,
        commit_stream,
        event_listener,
    })
}

fn take_me_out(my_replica_id: &str, cluster_members: &mut Vec<RaftMemberInfo>) -> Option<RaftMemberInfo> {
    let mut to_remove = None;

    for (i, member_info) in cluster_members.iter().enumerate() {
        if my_replica_id == &member_info.replica_id {
            to_remove = Some(i);
            break;
        }
    }

    to_remove.map(|i| cluster_members.remove(i))
}

async fn try_create_cluster_tracker(
    logger: slog::Logger,
    my_info: RaftMemberInfo,
    peers_info: Vec<RaftMemberInfo>,
) -> Result<ClusterTracker, RaftClientCreationError> {
    let my_replica_metadata = ReplicaMetadata::from(my_info);
    let peers_replica_metadata = peers_info.into_iter().map(ReplicaMetadata::from).collect();

    ClusterTracker::create_valid_cluster(logger, my_replica_metadata, peers_replica_metadata)
        .await
        .map_err(|e| RaftClientCreationError::InvalidClusterInfo(e.into()))
}

fn raft_rpc_server_addr(member_info: &RaftMemberInfo) -> SocketAddr {
    SocketAddr::V4(SocketAddrV4::new(
        member_info.ip_addr,
        member_info.raft_internal_rpc_port,
    ))
}
