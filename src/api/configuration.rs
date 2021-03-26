//! This mod is responsible for configuring and creating an instance of `RaftClientApi` for application to use.

use crate::api::factory::ClientCreationError;
use std::convert::TryFrom;
use std::net::Ipv4Addr;
use tokio::time::Duration;

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
    pub replica_ip_addr: Ipv4Addr,
    pub replica_port: u16,
}

#[derive(Clone, Default)]
pub struct RaftOptions {
    pub leader_heartbeat_duration: Option<Duration>,
    pub follower_min_timeout: Option<Duration>,
    pub follower_max_timeout: Option<Duration>,
    pub leader_append_entries_timeout: Option<Duration>,
}

pub(super) struct RaftOptionsValidated {
    pub leader_heartbeat_duration: Duration,
    pub follower_min_timeout: Duration,
    pub follower_max_timeout: Duration,
    pub leader_append_entries_timeout: Duration,
}

impl RaftOptionsValidated {
    fn validate(&self) -> Result<(), ClientCreationError> {
        if self.leader_heartbeat_duration >= self.follower_min_timeout {
            return Err(ClientCreationError::IllegalClientOptions(
                "Follower minimum timeout must be greater than leader's heartbeat".to_string(),
            ));
        }
        if self.follower_min_timeout >= self.follower_max_timeout {
            return Err(ClientCreationError::IllegalClientOptions(
                "Follower minimum timeout must be less than maximum timeout".to_string(),
            ));
        }
        if self.leader_append_entries_timeout >= self.follower_min_timeout {
            return Err(ClientCreationError::IllegalClientOptions(
                "Leader's AppendEntries RPC timeout must be less than the follower's heartbeat timeout".to_string(),
            ));
        }

        Ok(())
    }
}

impl TryFrom<RaftOptions> for RaftOptionsValidated {
    type Error = ClientCreationError;

    fn try_from(options: RaftOptions) -> Result<Self, Self::Error> {
        let values = RaftOptionsValidated {
            leader_heartbeat_duration: options.leader_heartbeat_duration.unwrap_or(Duration::from_millis(100)),
            follower_min_timeout: options.follower_min_timeout.unwrap_or(Duration::from_millis(500)),
            follower_max_timeout: options.follower_max_timeout.unwrap_or(Duration::from_millis(1500)),
            leader_append_entries_timeout: options
                .leader_append_entries_timeout
                .unwrap_or(Duration::from_millis(300)),
        };

        values.validate()?;
        Ok(values)
    }
}
