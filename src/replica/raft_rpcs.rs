use crate::commitlog::Index;
use crate::replica::local_state::Term;
use crate::ReplicaId;
use std::error::Error;
use std::io;
use std::net::Ipv4Addr;

// For external application to call into this library. Idk. I'm still trying to figure this out.
pub trait RaftClientApi {
    fn write_to_log(&mut self, input: WriteToLogInput)
        -> Result<WriteToLogOutput, WriteToLogError>;
}

pub struct WriteToLogInput {
    pub data: Vec<u8>,
}

pub struct WriteToLogOutput {
    pub applier_outcome: Vec<u8>,
}

#[derive(Debug, thiserror::Error)]
pub enum WriteToLogError {
    // Wait. This is kind of confusing. Client will also have to access cluster config. I need to
    // think about my service architecture. Maybe all nodes in cluster will have FE APIs accessed by
    // LB and all forward to leader. That doesn't reduce any work on leader though, and just adds
    // load on non-leaders. And adds LB complexity. Okay, probably a cluster config aware client.
    // :P DX
    #[error("I'm not leader")]
    FollowerRedirect {
        leader_id: ReplicaId,
        leader_ip: Ipv4Addr,
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

// We need this internally.
pub trait RaftRpcHandler {
    fn handle_request_vote(
        &mut self,
        input: RequestVoteInput,
    ) -> Result<RequestVoteOutput, RequestVoteError>;
    fn handle_append_entries(
        &mut self,
        input: AppendEntriesInput,
    ) -> Result<AppendEntriesOutput, AppendEntriesError>;
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
    pub leader_previous_log_entry_index: Index,
    pub leader_previous_log_entry_term: Term,
    pub leader_commit_index: Index,
    pub entries: Vec<LeaderLogEntry>,
}

pub struct LeaderLogEntry {
    pub term: Term,
    pub index: Index,
    pub data: Vec<u8>,
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
