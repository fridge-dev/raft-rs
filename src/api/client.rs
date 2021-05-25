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

    pub async fn enqueue_entry(&self, input: EnqueueEntryInput) -> Result<EnqueueEntryOutput, EnqueueEntryError> {
        let replica_input = replica::EnqueueForReplicationInput { data: input.data };

        self.actor_client
            .enqueue_for_replication(replica_input)
            .await
            .map(|o| o.into())
            .map_err(|e| e.into())
    }
}

#[derive(Debug)]
pub struct EnqueueEntryInput {
    // TODO:3 consider new-typing `EntryData(Bytes)` or whatever type we use internally (here and commit stream).
    pub data: Bytes,
}

#[derive(Debug)]
pub struct EnqueueEntryOutput {
    pub entry_id: EntryId,
}

// Opaque type for application to match CommittedEntry with.
#[derive(Debug, PartialEq)]
pub struct EntryId {
    pub(crate) term: Term,
    pub(crate) entry_index: Index,
}

#[derive(Debug, thiserror::Error)]
pub enum EnqueueEntryError {
    #[error("I'm not leader")]
    LeaderRedirect(LeaderInfo),

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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LeaderInfo {
    pub replica_id: String,
    pub ip: Ipv4Addr,
    pub info_blob: MemberInfoBlob,
}

/// We allow application layer to provide an arbitrary blob of info about each member
/// that will be returned back to the application layer if we leader-redirect the
/// application to that member.
#[derive(Copy, Clone, Debug, Eq, PartialOrd, PartialEq)]
pub struct MemberInfoBlob(u128);

impl MemberInfoBlob {
    pub fn new(blob: u128) -> Self {
        MemberInfoBlob(blob)
    }

    pub fn into_inner(self) -> u128 {
        self.0
    }
}

// ------- Conversions --------

impl From<replica::EnqueueForReplicationOutput> for EnqueueEntryOutput {
    fn from(internal_output: replica::EnqueueForReplicationOutput) -> Self {
        EnqueueEntryOutput {
            entry_id: EntryId {
                term: internal_output.enqueued_term,
                entry_index: internal_output.enqueued_index,
            },
        }
    }
}

impl From<replica::EnqueueForReplicationError> for EnqueueEntryError {
    fn from(internal_error: replica::EnqueueForReplicationError) -> Self {
        match internal_error {
            replica::EnqueueForReplicationError::LeaderRedirect(leader_info) => {
                EnqueueEntryError::LeaderRedirect(LeaderInfo::from(leader_info))
            }
            replica::EnqueueForReplicationError::NoLeader => EnqueueEntryError::NoLeader,
            replica::EnqueueForReplicationError::LocalIoError(e) => EnqueueEntryError::LocalIoError(e),
            replica::EnqueueForReplicationError::ActorExited => EnqueueEntryError::ReplicaExited,
        }
    }
}

impl From<replica::LeaderRedirectInfo> for LeaderInfo {
    fn from(leader: replica::LeaderRedirectInfo) -> Self {
        LeaderInfo {
            replica_id: leader.replica_id.into_inner(),
            ip: leader.ip_addr,
            info_blob: MemberInfoBlob::new(leader.replica_blob.into_inner()),
        }
    }
}
