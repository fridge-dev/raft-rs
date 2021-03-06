//! This mod is responsible for configuring and creating an instance of `RaftClientApi` for application to use.

use std::net::Ipv4Addr;
use tokio::time::Duration;

pub struct RaftClientConfig {
    // A directory where we can create files and sub-directories to support the commit log.
    pub commit_log_directory: String,
    pub info_logger: slog::Logger,
    pub cluster_info: ClusterInfo,
    // TODO:1 make optional?
    pub leader_heartbeat_duration: Duration,
    pub follower_min_timeout: Duration,
    pub follower_max_timeout: Duration,
}

#[derive(Clone)]
pub struct ClusterInfo {
    pub my_replica_id: String,
    pub cluster_members: Vec<MemberInfo>,
}

#[derive(Clone)]
pub struct MemberInfo {
    pub replica_id: String,
    pub replica_ip_addr: Ipv4Addr,
    pub replica_port: u16,
}
