use crate::commitlog::Index;
use crate::replica::local_state::Term;
use crate::replica::peers::ReplicaId;
use bytes::Bytes;
use std::io;

// We implement this to support the {raft_endpoint} gRPC server.
pub trait RaftRpcHandler {
    fn handle_request_vote(&mut self, input: RequestVoteInput) -> Result<RequestVoteOutput, RequestVoteError>;
    fn handle_append_entries(&mut self, input: AppendEntriesInput) -> Result<AppendEntriesOutput, AppendEntriesError>;
}

pub struct RequestVoteInput {
    pub candidate_term: Term,
    pub candidate_id: ReplicaId,
    pub candidate_last_log_entry_index: Index,
    pub candidate_last_log_entry_term: Term,
}

pub struct RequestVoteOutput {
    pub vote_granted: bool,
}

pub enum RequestVoteError {
    CandidateNotInCluster,
    RequestTermOutOfDate(TermOutOfDateInfo),
}

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
pub struct ReplicatedLogEntry {
    pub term: Term,
    pub data: Bytes,
}

pub struct AppendEntriesOutput {
    // Nothing
}

pub enum AppendEntriesError {
    ClientNotInCluster,
    ClientTermOutOfDate(TermOutOfDateInfo),
    ServerMissingPreviousLogEntry,
    ServerIoError(io::Error),
}

pub struct TermOutOfDateInfo {
    pub current_term: Term,
}
