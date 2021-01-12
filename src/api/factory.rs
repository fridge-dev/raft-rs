use crate::api::placeholder_impl::PlaceholderImpl;
use crate::commitlog::{InMemoryLogFactory, LogConfig, LogFactory};
use crate::replica::{Cluster, Replica, ReplicaConfig, VolatileLocalState};
use crate::{LocalStateMachineApplier, RaftClientApi, RaftClientConfig};
use std::convert::TryFrom;
use std::error::Error;
use std::io;

pub fn create_raft_client<M: 'static>(
    config: RaftClientConfig<M>,
) -> Result<Box<dyn RaftClientApi>, ClientCreationError>
where
    M: LocalStateMachineApplier,
{
    let log = InMemoryLogFactory::new()
        .try_create_log(LogConfig {
            base_directory: config.log_directory,
        })
        .map_err(|e| ClientCreationError::LogInitialization(e))?;

    let cluster = Cluster::try_from(config.cluster_info)?;

    let replica = Replica::new(ReplicaConfig {
        cluster,
        log,
        local_state: VolatileLocalState::new(),
        state_machine: config.state_machine,
    });

    let client = PlaceholderImpl { replica };

    Ok(Box::new(client))
}

#[derive(Debug, thiserror::Error)]
pub enum ClientCreationError {
    #[error("Invalid cluster info")]
    InvalidClusterInfo(Box<dyn Error>),
    #[error("Log initialization failure")]
    LogInitialization(io::Error),
}
