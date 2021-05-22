use crate::actor::ActorClient;
use crate::commitlog::Index;
use crate::replica;
use crate::replica::Term;
use bytes::Bytes;
use std::io;
use std::net::Ipv4Addr;

/// ReplicatedLog is the replicated log for external application to append to.
pub struct ReplicatedLog {
    actor_client: ActorClient,
}

impl ReplicatedLog {
    pub(crate) fn new(actor_client: ActorClient) -> Self {
        ReplicatedLog { actor_client }
    }

    pub async fn start_replication(
        &self,
        input: StartReplicationInput,
    ) -> Result<StartReplicationOutput, StartReplicationError> {
        let replica_input = replica::EnqueueForReplicationInput { data: input.data };

        self.actor_client
            .enqueue_for_replication(replica_input)
            .await
            .map(|o| StartReplicationOutput {
                key: EntryKey {
                    term: o.enqueued_term,
                    entry_index: o.enqueued_index,
                },
            })
            .map_err(|e| e.into())
    }
}

#[derive(Debug)]
pub struct StartReplicationInput {
    pub data: Bytes,
}

#[derive(Debug)]
pub struct StartReplicationOutput {
    pub key: EntryKey,
}

// Opaque type for application to match CommittedEntry with.
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
        leader_blob: MemberInfoBlob,
    },

    // Can be retried with exponential backoff with recommended initial delay of 200ms. Likely an
    // election is in progress.
    #[error("Cluster is in a tough shape. No one is leader.")]
    NoLeader,

    // Might be unneeded, if Replica event loop doesn't sync write to disk.
    #[error("Failed to persist log")]
    LocalIoError(io::Error),

    // Replica logic runs on a background task. This error is returned if the task has exited.
    #[error("Replica task has exited")]
    ReplicaExited,
}

impl From<replica::EnqueueForReplicationError> for StartReplicationError {
    fn from(internal_error: replica::EnqueueForReplicationError) -> Self {
        match internal_error {
            replica::EnqueueForReplicationError::LeaderRedirect {
                leader_id,
                leader_ip,
                leader_blob,
            } => StartReplicationError::LeaderRedirect {
                leader_id: leader_id.into_inner(),
                leader_ip,
                leader_blob: MemberInfoBlob::new(leader_blob.into_inner()),
            },
            replica::EnqueueForReplicationError::NoLeader => StartReplicationError::NoLeader,
            replica::EnqueueForReplicationError::LocalIoError(e2) => StartReplicationError::LocalIoError(e2),
            replica::EnqueueForReplicationError::ActorExited => StartReplicationError::ReplicaExited,
        }
    }
}

/// We allow application layer to provide an arbitrary blob of info about each member
/// that will be returned back to the application layer if we leader-redirect the
/// application to that member.
#[derive(Copy, Clone, Debug, PartialOrd, PartialEq)]
pub struct MemberInfoBlob(u128);

impl MemberInfoBlob {
    pub fn new(blob: u128) -> Self {
        MemberInfoBlob(blob)
    }

    pub fn into_inner(self) -> u128 {
        self.0
    }
}
