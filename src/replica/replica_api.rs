use crate::commitlog::Index;
use crate::replica::local_state::Term;
use crate::replica::peers::ReplicaId;
use bytes::Bytes;
use std::io;
use std::net::Ipv4Addr;

#[derive(Debug)]
pub struct WriteToLogInput {
    pub data: Bytes,
}

#[derive(Debug)]
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
    ReplicationError(/* TODO */),
}

#[derive(Debug)]
pub struct RequestVoteInput {
    pub candidate_term: Term,
    pub candidate_id: ReplicaId,
    pub candidate_last_log_entry_index: Index,
    pub candidate_last_log_entry_term: Term,
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
}

#[derive(Debug)]
pub struct AppendEntriesInput {
    pub leader_term: Term,
    pub leader_id: ReplicaId,
    pub leader_commit_index: Index,
    pub new_entries: Vec<ReplicatedLogEntry>,
    // > index of log entry immediately preceding new ones
    pub leader_previous_log_entry_index: Index, // TODO:2 this is type from commitlog crate. Bad abstraction.
    pub leader_previous_log_entry_term: Term,
}

// TODO:3 name is weird, make it better
#[derive(Debug)]
pub struct ReplicatedLogEntry {
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
}

#[derive(Debug)]
pub struct TermOutOfDateInfo {
    pub current_term: Term,
}

#[derive(Debug)]
pub struct RequestVoteResultFromPeerInput; // TODO:2

#[derive(Debug)]
pub struct AppendEntriesResultFromPeerInput; // TODO:2
