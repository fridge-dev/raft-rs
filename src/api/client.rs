use bytes::Bytes;
use std::error::Error;
use std::io;
use std::net::Ipv4Addr;

// For external application to call into this library.
pub trait RaftClientApi {
    fn write_to_log(&mut self, input: WriteToLogInput) -> Result<WriteToLogOutput, WriteToLogError>;
}

pub struct WriteToLogInput {
    pub data: Bytes,
}

pub struct WriteToLogOutput {
    pub applier_outcome: Bytes,
}

#[derive(Debug, thiserror::Error)]
pub enum WriteToLogError {
    #[error("I'm not leader")]
    LeaderRedirect { leader_id: String, leader_ip: Ipv4Addr },

    // Can be retried with exponential backoff with recommended initial delay of 200ms. Likely an
    // election is in progress.
    #[error("Cluster is in a tough shape. No one is leader.")]
    NoLeader,

    #[error("Failed to persist log")]
    LocalIoError(io::Error),

    // For unexpected failures.
    #[error("I'm leader, but couldn't replicate data to majority. Some unexpected failure. Idk.")]
    ReplicationError(Box<dyn Error>),
}
