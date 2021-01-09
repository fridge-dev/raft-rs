//! This mod is responsible for configuring and creating an instance of `RaftClientApi` for application to use.

use crate::api::client::RaftClientApi;
use crate::api::placeholder_impl::PlaceholderImpl;
use crate::api::state_machine::LocalStateMachineApplier;
use crate::commitlog::{InMemoryLogFactory, LogConfig, LogFactory};
use crate::replica::{Replica, ReplicaConfig, VolatileLocalState};
use crate::{ClusterConfig, ReplicaId};

pub struct RaftConfig<M>
where
    M: LocalStateMachineApplier,
{
    pub state_machine: M,
    pub cluster_config: ClusterConfig,
    pub my_replica_id: ReplicaId,
    // A directory where we can create files and sub-directories.
    pub log_directory: String,
}

pub fn create_raft_client<M: 'static>(config: RaftConfig<M>) -> Box<dyn RaftClientApi>
where
    M: LocalStateMachineApplier,
{
    let log = InMemoryLogFactory::new()
        .try_create_log(LogConfig {
            base_directory: config.log_directory,
        })
        .expect("that shit can't fail");

    let replica = Replica::new(ReplicaConfig {
        my_replica_id: config.my_replica_id,
        cluster_members: config.cluster_config.cluster_members,
        log,
        local_state: VolatileLocalState::new(),
        state_machine: config.state_machine,
    });

    let client = PlaceholderImpl { replica };

    Box::new(client)
}
