use crate::api::client::ClientAdapter;
use crate::commitlog::InMemoryLog;
use crate::replica::{Cluster, Replica, ReplicaConfig, VolatileLocalState};
use crate::{api, replica, LocalStateMachineApplier, RaftClientConfig, ReplicatedStateMachine};
use std::error::Error;
use std::io;

pub async fn create_raft_client<M: 'static>(
    config: RaftClientConfig<M>,
) -> Result<Box<dyn ReplicatedStateMachine<M>>, ClientCreationError>
where
    M: LocalStateMachineApplier,
{
    let log = InMemoryLog::create().map_err(|e| ClientCreationError::LogInitialization(e))?;

    let cluster = try_create_cluster(config.cluster_info).await?;

    let replica = Replica::new(ReplicaConfig {
        cluster,
        log,
        local_state: VolatileLocalState::new(),
        state_machine: config.state_machine,
    });

    let client = ClientAdapter { replica };

    Ok(Box::new(client))
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

async fn try_create_cluster(cluster_info: api::ClusterInfo) -> Result<replica::Cluster, ClientCreationError> {
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

    Cluster::create_valid_cluster(my_md, peers_md)
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
