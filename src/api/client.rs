use crate::actor::ActorClient;
use crate::commitlog::Index;
use crate::replica;
use crate::replica::Term;
use bytes::Bytes;
use std::io;
use std::net::Ipv4Addr;
use tokio::sync::mpsc;

pub fn create_commit_stream() -> (CommitStreamPublisher, CommitStream) {
    let (tx, rx) = mpsc::unbounded_channel();

    let applier_sender = CommitStreamPublisher { sender: tx };
    let applier_receiver = CommitStream { receiver: rx };

    (applier_sender, applier_receiver)
}

pub struct CommitStreamPublisher {
    sender: mpsc::UnboundedSender<CommittedEntry>,
}

impl CommitStreamPublisher {
    pub fn notify_commit(&self, result: CommittedEntry) {
        if let Err(_) = self.sender.send(result) {
            println!("CommitStream has disconnected.");
        }
    }
}

// For external application to call into this library.
pub struct CommitStream {
    receiver: mpsc::UnboundedReceiver<CommittedEntry>,
}

pub struct CommittedEntry {
    pub key: EntryKey,
    pub data: Bytes,
}

impl CommitStream {
    // TODO:3 How to handle when we accepted entry for repl, then lost leadership?
    //        Do we just *not* inform app layer?
    /// next returns the next committed entry to be applied to your application's state machine.
    pub async fn next(&mut self) -> CommittedEntry {
        self.receiver
            .recv()
            .await
            .expect("Replica event loop should never exit.")
    }
}

pub fn new_replicated_log(actor_client: ActorClient) -> ReplicatedLog {
    ReplicatedLog {
         actor_client,
    }
}

/// ReplicatedLog is the replicated log for external application to append to.
pub struct ReplicatedLog {
    actor_client: ActorClient,
}

#[derive(Debug)]
pub struct StartReplicationInput {
    pub data: Bytes,
}

#[derive(Debug)]
pub struct StartReplicationOutput {
    pub key: EntryKey,
}

// Opaque type for application to match EndReplicationMessage with.
#[derive(Debug, PartialEq)]
pub struct EntryKey {
    pub(crate) term: Term,
    pub(crate) entry_index: Index,
}

#[derive(Debug, thiserror::Error)]
pub enum StartReplicationError {
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

    // Might be unneeded, if Replica event loop doesn't sync write to disk.
    #[error("Failed to persist log")]
    LocalIoError(io::Error),
}

impl ReplicatedLog {
    pub async fn start_replication(
        &self,
        input: StartReplicationInput,
    ) -> Result<StartReplicationOutput, StartReplicationError> {
        let replica_input = replica::EnqueueForReplicationInput { data: input.data };

        self.actor_client
            .write_to_log(replica_input)
            .await
            .map(|o| StartReplicationOutput {
                key: EntryKey {
                    term: o.enqueued_term,
                    entry_index: o.enqueued_index,
                },
            })
            .map_err(|e| match e {
                replica::EnqueueForReplicationError::LeaderRedirect {
                    leader_id,
                    leader_ip,
                    leader_port,
                } => StartReplicationError::LeaderRedirect {
                    leader_id,
                    leader_ip,
                    leader_port,
                },
                replica::EnqueueForReplicationError::NoLeader => StartReplicationError::NoLeader,
                replica::EnqueueForReplicationError::LocalIoError(e2) => StartReplicationError::LocalIoError(e2),
            })
    }
}
