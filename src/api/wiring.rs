use crate::actor::{ActorClient, ReplicaActor};
use crate::api::client::RaftClient;
use crate::api::options::RaftOptionsValidated;
use crate::api::types::RaftMemberInfo;
use crate::commitlog::InMemoryLog;
use crate::replica::{ReplicaId, ReplicaMetadata};
use crate::server::RpcServer;
use crate::{replica, server, RaftCommitStream};
use crate::{RaftEventListener, RaftOptions, RaftReplicatedLog};
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

pub async fn try_create_raft_client(config: RaftClientConfig) -> Result<RaftClient, RaftClientCreationError> {
    let root_logger = config.info_logger;

    let commit_log =
        InMemoryLog::create(root_logger.clone()).map_err(|e| RaftClientCreationError::LogInitialization(e))?;

    let my_member_info = my_info(&config.my_replica_id, &config.cluster_members)
        .ok_or_else(|| RaftClientCreationError::MeNotInCluster)?;
    let my_server_addr = raft_rpc_server_addr(&my_member_info);

    let cluster_members = config.cluster_members.into_iter().map(ReplicaMetadata::from).collect();

    let (actor_client, actor_queue_rx) = ActorClient::new(10);

    let options = RaftOptionsValidated::try_from(config.options)
        .map_err(|e| RaftClientCreationError::IllegalClientOptions(e.to_string()))?;

    let (server_shutdown_handle, server_shutdown_signal) = server::shutdown_signal();

    let (replica, replica_commit_stream, election_state_change_listener) = replica::create_replica(
        root_logger.clone(),
        ReplicaId::new(config.my_replica_id),
        cluster_members,
        commit_log,
        server_shutdown_handle,
        actor_client.weak(),
        options.leader_heartbeat_duration,
        options.follower_min_timeout,
        options.follower_max_timeout,
        options.leader_append_entries_timeout,
    )
    .map_err(|e| RaftClientCreationError::InvalidClusterInfo(e.into()))?;

    let replica_actor = ReplicaActor::new(root_logger.clone(), actor_queue_rx, replica);
    tokio::spawn(replica_actor.run_event_loop());

    let replica_raft_server = RpcServer::new(root_logger.clone(), actor_client.weak());
    tokio::spawn(replica_raft_server.run(my_server_addr, server_shutdown_signal));

    let replicated_log = RaftReplicatedLog::new(actor_client);
    let commit_stream = RaftCommitStream::new(replica_commit_stream);
    let event_listener = RaftEventListener::new(election_state_change_listener);

    Ok(RaftClient {
        replicated_log,
        commit_stream,
        event_listener,
    })
}

fn my_info<'a>(my_replica_id: &'_ str, cluster_members: &'a Vec<RaftMemberInfo>) -> Option<&'a RaftMemberInfo> {
    for member_info in cluster_members {
        if my_replica_id == &member_info.replica_id {
            return Some(member_info);
        }
    }

    None
}

fn raft_rpc_server_addr(member_info: &RaftMemberInfo) -> SocketAddr {
    SocketAddr::V4(SocketAddrV4::new(
        member_info.ip_addr,
        member_info.raft_internal_rpc_port,
    ))
}
