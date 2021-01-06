use crate::commitlog::Index;
use crate::replica::local_state::Term;
use crate::ReplicaId;

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
    RequestTermOutOfDate(TermOutOfDateInfo),
    CandidateNotInCluster,
}

pub struct AppendEntriesInput {
    pub leader_term: Term,
    pub leader_id: ReplicaId,
    pub leader_previous_log_entry_index: Index,
    pub leader_previous_log_entry_term: Term,
    pub leader_commit_index: Index,
}

pub struct AppendEntriesOutput {
    // Nothing
}

pub enum AppendEntriesError {
    RequestTermOutOfDate(TermOutOfDateInfo),
    FollowerMissingPreviousLogEntry,
}

pub struct TermOutOfDateInfo {
    pub current_term: Term,
}
