use std::convert::TryFrom;
use tokio::time::Duration;

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
    fn validate(&self) -> Result<(), &'static str> {
        if self.leader_heartbeat_duration >= self.follower_min_timeout {
            return Err("Follower minimum timeout must be greater than leader's heartbeat");
        }
        if self.follower_min_timeout >= self.follower_max_timeout {
            return Err("Follower minimum timeout must be less than maximum timeout");
        }
        if self.leader_append_entries_timeout >= self.follower_min_timeout {
            return Err("Leader's AppendEntries RPC timeout must be less than the follower's heartbeat timeout");
        }

        Ok(())
    }
}

impl TryFrom<RaftOptions> for RaftOptionsValidated {
    type Error = &'static str;

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
