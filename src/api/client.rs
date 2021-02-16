use crate::api::state_machine::LocalStateMachineApplier;
use crate::commitlog::Log;
use crate::replica::{PersistentLocalState, RaftLogEntry, Replica};
use bytes::Bytes;
use std::error::Error;
use std::io;
use std::net::Ipv4Addr;

// For external application to call into this library.
pub trait ReplicatedStateMachine<M>
where
    M: LocalStateMachineApplier,
{
    fn execute(&mut self, input: WriteToLogInput) -> Result<WriteToLogOutput, WriteToLogError>;
    fn local_state_machine(&self) -> &M;
}

pub struct WriteToLogInput {
    pub data: Bytes,
}

pub struct WriteToLogOutput {
    pub applier_output: Bytes,
}

#[derive(Debug, thiserror::Error)]
pub enum WriteToLogError {
    #[error("I'm not leader")]
    LeaderRedirect {
        leader_id: String,
        leader_ip: Ipv4Addr,
        leader_port: u16,
    },

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

/// ClientAdapter adapts the `Replica` logic to the `ReplicatedStateMachine` interface.
pub struct ClientAdapter<L, S, M>
where
    L: Log<RaftLogEntry>,
    S: PersistentLocalState,
    M: LocalStateMachineApplier,
{
    pub replica: Replica<L, S, M>,
}

impl<L, S, M> ReplicatedStateMachine<M> for ClientAdapter<L, S, M>
where
    L: Log<RaftLogEntry>,
    S: PersistentLocalState,
    M: LocalStateMachineApplier,
{
    fn execute(&mut self, input: WriteToLogInput) -> Result<WriteToLogOutput, WriteToLogError> {
        self.replica.write_to_log(input)
    }

    fn local_state_machine(&self) -> &M {
        self.replica.local_state_machine()
    }
}
