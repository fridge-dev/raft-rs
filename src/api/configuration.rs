//! This mod is responsible for configuring and creating an instance of `RaftClientApi` for application to use.

use crate::api::state_machine::LocalStateMachineApplier;
use std::net::Ipv4Addr;
use crate::replica;
use std::convert::TryFrom;
use crate::api::factory::ClientCreationError;
use crate::replica::Cluster;

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
}

impl TryFrom<ClusterInfo> for replica::Cluster {
    type Error = ClientCreationError;

    fn try_from(cluster_info: ClusterInfo) -> Result<Self, Self::Error> {
        let members = cluster_info.cluster_members.into_iter()
            .map(|member| member.into())
            .collect();

        Cluster::create_valid_cluster(members, cluster_info.my_replica_id)
            .map_err(|e| ClientCreationError::InvalidClusterInfo(e.into()))
    }
}

impl From<MemberInfo> for replica::ClusterMember {
    fn from(member_info: MemberInfo) -> Self {
        replica::ClusterMember::new(
            member_info.replica_id,
            member_info.replica_ip_addr,
        )
    }
}