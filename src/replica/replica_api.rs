use crate::commitlog::Index;
use crate::replica::local_state::Term;
use crate::replica::peers::{ReplicaId, ReplicaInfoBlob};
use bytes::Bytes;
use std::io;
use std::net::Ipv4Addr;

#[derive(Debug)]
pub(crate) struct EnqueueForReplicationInput {
    pub(crate) data: Bytes,
}

#[derive(Debug)]
pub(crate) struct EnqueueForReplicationOutput {
    pub(crate) enqueued_term: Term,
    // TODO:2.5 Index is type from commitlog crate. Bad abstraction. Fix it.
    pub(crate) enqueued_index: Index,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum EnqueueForReplicationError {
    #[error("I'm not leader")]
    LeaderRedirect(LeaderRedirectInfo),

    // Can be retried with exponential backoff with recommended initial delay of 200ms. Likely an
    // election is in progress.
    #[error("Cluster is in a tough shape. No one is leader.")]
    NoLeader,

    #[error("Failed to persist log")]
    LocalIoError(io::Error),

    #[error("Replica actor is dead RIP")]
    ActorExited,
}

#[derive(Debug, Clone)]
pub(crate) struct LeaderRedirectInfo {
    pub(crate) replica_id: ReplicaId,
    pub(crate) ip_addr: Ipv4Addr,
    pub(crate) replica_blob: ReplicaInfoBlob,
}

#[derive(Debug)]
pub(crate) struct RequestVoteInput {
    pub(crate) candidate_term: Term,
    pub(crate) candidate_id: ReplicaId,
    // TODO:2.5 Index is type from commitlog crate. Bad abstraction. Fix it.
    pub(crate) candidate_last_log_entry: Option<(Term, Index)>,
}

#[derive(Debug)]
pub(crate) struct RequestVoteOutput {
    pub(crate) vote_granted: bool,
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum RequestVoteError {
    #[error("Requesting candidate is not in the cluster")]
    CandidateNotInCluster,
    #[error("Requesting candidate's term is out of date")]
    RequestTermOutOfDate(TermOutOfDateInfo),
    #[error("We (server) are unavailable because actor is dead RIP")]
    ActorExited,
}

#[derive(Debug)]
pub(crate) struct AppendEntriesInput {
    pub(crate) leader_term: Term,
    pub(crate) leader_id: ReplicaId,
    // "Previous log entry" is the log entry immediately preceding the new ones in AppendEntriesInput.
    // TODO:2.5 Index is type from commitlog crate. Bad abstraction. Fix it.
    pub(crate) leader_previous_log_entry: Option<(Term, Index)>,
    // TODO:2.5 Index is type from commitlog crate. Bad abstraction. Fix it.
    pub(crate) leader_commit_index: Option<Index>,
    pub(crate) new_entries: Vec<AppendEntriesLogEntry>,
}

#[derive(Debug)]
pub(crate) struct AppendEntriesLogEntry {
    pub(crate) term: Term,
    pub(crate) data: Bytes,
}

#[derive(Debug)]
pub(crate) struct AppendEntriesOutput {
    // Nothing
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum AppendEntriesError {
    #[error("Client is not in cluster")]
    ClientNotInCluster,
    #[error("Client's term is out of date")]
    ClientTermOutOfDate(TermOutOfDateInfo),
    #[error("We (server) are missing previous log entry")]
    ServerMissingPreviousLogEntry,
    #[error("We (server) had an IO failure: {0:?}")]
    ServerIoError(io::Error),
    #[error("We (server) are unavailable because actor is dead RIP")]
    ActorExited,
}

#[derive(Debug)]
pub(crate) struct TermOutOfDateInfo {
    pub(crate) current_term: Term,
}

#[derive(Debug)]
pub(crate) struct RequestVoteReplyFromPeer {
    pub(crate) peer_id: ReplicaId,
    pub(crate) term: Term,
    pub(crate) result: RequestVoteResult,
}

#[derive(Debug)]
pub(crate) enum RequestVoteResult {
    VoteGranted,
    VoteNotGranted,
    RetryableFailure,
    MalformedReply,
}

#[derive(Debug)]
pub(crate) struct AppendEntriesReplyFromPeer {
    pub(crate) descriptor: AppendEntriesReplyFromPeerDescriptor,
    pub(crate) result: Result<(), AppendEntriesReplyFromPeerError>,
}

// This is basically info about the original request
#[derive(Debug)]
pub(crate) struct AppendEntriesReplyFromPeerDescriptor {
    pub(crate) peer_id: ReplicaId,
    pub(crate) term: Term,
    pub(crate) seq_no: u64,
    pub(crate) previous_log_entry_index: Option<Index>,
    pub(crate) num_log_entries: usize,
}

#[derive(Debug)]
pub(crate) enum AppendEntriesReplyFromPeerError {
    PeerMissingPreviousLogEntry,
    RetryableFailure(String),
    StaleTerm { new_term: Term },
}

/// LeaderTimerTick contains info for a single tick of a leader's per-peer timer.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct LeaderTimerTick {
    pub(crate) peer_id: ReplicaId,
    pub(crate) term: Term,
}
