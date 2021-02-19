use crate::api::state_machine::LocalStateMachineApplier;
use bytes::Bytes;
use std::io;
use std::net::Ipv4Addr;
use crate::actor::ActorClient;
use crate::replica;

// For external application to call into this library.
// TODO:2 Pretty much everything in this file needs rename. :P For now just make it distinct.
#[async_trait::async_trait]
pub trait ReplicatedStateMachine<M>
where
    M: LocalStateMachineApplier + Send,
{
    async fn execute(&self, input: WriteToLogApiInput) -> Result<WriteToLogApiOutput, WriteToLogApiError>;
    fn local_state_machine(&self) -> &M; // Need better read API. I won't be able to do this.
}

#[derive(Debug)]
pub struct WriteToLogApiInput {
    pub data: Bytes,
}

#[derive(Debug)]
pub struct WriteToLogApiOutput {
    pub applier_output: Bytes,
}

#[derive(Debug, thiserror::Error)]
pub enum WriteToLogApiError {
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
    ReplicationError(/* TODO */),
}

/// ClientAdapter adapts the `ActorClient` API to the `ReplicatedStateMachine` API.
pub struct ClientAdapter {
    pub actor_client: ActorClient,
}

#[async_trait::async_trait]
impl<M: 'static> ReplicatedStateMachine<M> for ClientAdapter
where
    M: LocalStateMachineApplier + Send,
{
    async fn execute(&self, input: WriteToLogApiInput) -> Result<WriteToLogApiOutput, WriteToLogApiError> {
        let replica_input = replica::WriteToLogInput {
            data: input.data,
        };

        self.actor_client.write_to_log(replica_input)
            .await
            .map(|o| WriteToLogApiOutput {
                applier_output: o.applier_output,
            })
            .map_err(|e| match e {
                replica::WriteToLogError::LeaderRedirect { leader_id, leader_ip, leader_port } => WriteToLogApiError::LeaderRedirect {
                    leader_id,
                    leader_ip,
                    leader_port,
                },
                replica::WriteToLogError::NoLeader => WriteToLogApiError::NoLeader,
                replica::WriteToLogError::LocalIoError(e2) => WriteToLogApiError::LocalIoError(e2),
                replica::WriteToLogError::ReplicationError() => WriteToLogApiError::ReplicationError(),
            })
    }

    fn local_state_machine(&self) -> &M {
        unimplemented!()
    }
}
