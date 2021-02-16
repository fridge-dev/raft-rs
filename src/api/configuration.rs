//! This mod is responsible for configuring and creating an instance of `RaftClientApi` for application to use.

use crate::api::state_machine::LocalStateMachineApplier;
use std::net::Ipv4Addr;

pub struct RaftClientConfig<M>
where
    M: LocalStateMachineApplier,
{
    pub state_machine: M,
    // A directory where we can create files and sub-directories.
    pub log_directory: String,
    pub cluster_info: ClusterInfo,
}

pub struct ClusterInfo {
    pub my_replica_id: String,
    pub cluster_members: Vec<MemberInfo>,
}

pub struct MemberInfo {
    pub replica_id: String,
    pub replica_ip_addr: Ipv4Addr,
    pub replica_port: u16,
}
