//! This mod is responsible for configuring and creating an instance of `RaftClientApi` for application to use.

use std::net::Ipv4Addr;

pub struct RaftClientConfig {
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
