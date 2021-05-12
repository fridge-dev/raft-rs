use crate::commitlog::Index;
use crate::replica::local_state::Term;
use crate::replica::peers::{ReplicaBlob, ReplicaId};
use bytes::Bytes;
use std::io;
use std::net::Ipv4Addr;

#[derive(Debug)]
pub struct EnqueueForReplicationInput {
    pub data: Bytes,
}

#[derive(Debug)]
pub struct EnqueueForReplicationOutput {
    pub enqueued_term: Term,
    pub enqueued_index: Index,
}

#[derive(Debug, thiserror::Error)]
pub enum EnqueueForReplicationError {
    #[error("I'm not leader")]
    LeaderRedirect {
        leader_id: ReplicaId,
        leader_ip: Ipv4Addr,
        leader_blob: ReplicaBlob,
    },

    // Can be retried with exponential backoff with recommended initial delay of 200ms. Likely an
    // election is in progress.
    #[error("Cluster is in a tough shape. No one is leader.")]
    NoLeader,

    #[error("Failed to persist log")]
    LocalIoError(io::Error),
}

#[derive(Debug)]
pub struct RequestVoteInput {
    pub candidate_term: Term,
    pub candidate_id: ReplicaId,
    pub candidate_last_log_entry: Option<(Term, Index)>,
}

#[derive(Debug)]
pub struct RequestVoteOutput {
    pub vote_granted: bool,
}

#[derive(thiserror::Error, Debug)]
pub enum RequestVoteError {
    #[error("Requesting candidate is not in the cluster")]
    CandidateNotInCluster,
    #[error("Requesting candidate's term is out of date")]
    RequestTermOutOfDate(TermOutOfDateInfo),
    #[error("We (server) are unavailable because actor is dead RIP")]
    ActorDead,
}

#[derive(Debug)]
pub struct AppendEntriesInput {
    pub leader_term: Term,
    pub leader_id: ReplicaId,
    pub leader_log_state: LeaderLogState,
    pub new_entries: Vec<AppendEntriesLogEntry>,
}

// This is essentially a matrix of optional prev entry and optional commit index, but removes
// the impossibility of Some(commit index) and None(prev entry). This changes an application layer
// panic to an RPC layer InvalidArgument response. The variant names can probably be better :P.
#[derive(Debug, Copy, Clone)]
pub enum LeaderLogState {
    Empty,
    NoCommit {
        previous_log_entry_term: Term,
        // TODO:2 this is type from commitlog crate. Bad abstraction.
        previous_log_entry_index: Index,
    },
    Normal {
        previous_log_entry_term: Term,
        // TODO:2 this is type from commitlog crate. Bad abstraction.
        previous_log_entry_index: Index,
        commit_index: Index,
    },
}

impl LeaderLogState {
    // "Previous log entry" is the log entry immediately preceding the new ones in AppendEntriesInput.
    pub fn previous_log_entry(&self) -> Option<(Term, Index)> {
        match self {
            LeaderLogState::Empty => None,
            LeaderLogState::NoCommit {
                previous_log_entry_term,
                previous_log_entry_index,
            } => Some((*previous_log_entry_term, *previous_log_entry_index)),
            LeaderLogState::Normal {
                previous_log_entry_term,
                previous_log_entry_index,
                ..
            } => Some((*previous_log_entry_term, *previous_log_entry_index)),
        }
    }
}

#[derive(Debug)]
pub struct AppendEntriesLogEntry {
    pub term: Term,
    pub data: Bytes,
}

#[derive(Debug)]
pub struct AppendEntriesOutput {
    // Nothing
}

#[derive(thiserror::Error, Debug)]
pub enum AppendEntriesError {
    #[error("Client is not in cluster")]
    ClientNotInCluster,
    #[error("Client's term is out of date")]
    ClientTermOutOfDate(TermOutOfDateInfo),
    #[error("We (server) are missing previous log entry")]
    ServerMissingPreviousLogEntry,
    #[error("We (server) had an IO failure: {0:?}")]
    ServerIoError(io::Error),
    #[error("We (server) are unavailable because actor is dead RIP")]
    ActorDead,
}

#[derive(Debug)]
pub struct TermOutOfDateInfo {
    pub current_term: Term,
}

#[derive(Debug)]
pub struct RequestVoteReplyFromPeer {
    pub peer_id: ReplicaId,
    pub term: Term,
    pub result: RequestVoteResult,
}

#[derive(Debug)]
pub enum RequestVoteResult {
    VoteGranted,
    VoteNotGranted,
    RetryableFailure,
    MalformedReply,
}

#[derive(Debug)]
pub struct AppendEntriesReplyFromPeer {
    pub descriptor: AppendEntriesReplyFromPeerDescriptor,
    pub result: Result<(), AppendEntriesReplyFromPeerError>,
}

// This is basically info about the original request
#[derive(Debug)]
pub struct AppendEntriesReplyFromPeerDescriptor {
    pub peer_id: ReplicaId,
    pub term: Term,
    pub seq_no: u64,
    pub previous_log_entry_index: Option<Index>,
    pub num_log_entries: usize,
}

#[derive(Debug)]
pub enum AppendEntriesReplyFromPeerError {
    PeerMissingPreviousLogEntry,
    RetryableFailure(String),
    StaleTerm { new_term: Term },
}

/// LeaderTimerTick contains info for a single tick of a leader's per-peer timer.
#[derive(Debug, Clone, PartialEq)]
pub struct LeaderTimerTick {
    pub peer_id: ReplicaId,
    pub term: Term,
}
